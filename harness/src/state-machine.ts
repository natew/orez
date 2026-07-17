// electric-style generated lifecycle model for the Rust hosts. A deterministic
// trace mixes writes, desired-query changes, retention pruning, lost responses,
// engine faults, server restarts, and client restarts. Every operation compares
// live client views to an authoritative SQL oracle. Failures emit the seed, full
// trace, and a delta-debugged reproducer under harness/regressions/.
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { canonical } from './canonical.js'
import { mutators, queries } from './fixture.js'
import { observedSyncFetch, type SyncHttpObservation } from './observed-fetch.js'
import { persistentKVStoreProvider } from './persistent-kv.js'
import { assertServerOutcome } from './server-outcome.js'

import type { FixtureZero, SyncTarget } from './target.js'

type StateTarget = SyncTarget & {
  readonly origin?: string
  readonly adminKey?: string
  dropNextPushResponse(): Promise<void> | void
  pull(): Promise<void>
  restart(downForMs?: number): Promise<void>
}

type FaultPoint =
  | 'push_before_mutation'
  | 'push_after_write_before_commit'
  | 'push_after_commit_before_response'
  | 'pull_during_tx'
  | 'pull_after_commit'
type FaultKind = 'kill' | 'error' | 'quota'

type FaultReceipt = {
  id: string
  arm: {
    step: number
    point: FaultPoint
    kind: FaultKind
    confirmed: true
  }
  resolution?: {
    status: 'fired' | 'not-fired'
    step: number
    operation: Operation['kind'] | 'run-end'
    reason: string
  }
}

// a second, independent fault class: a client-side transport pause that gates
// this client's pulls at the fetch seam. it is held open across other faults
// (an engine-fault arm and a server restart) so two classes are active at once,
// then healed on resume. its own arm/fire/heal receipts are validated exactly
// like the engine-fault receipts: a generated schedule must fire and heal every
// pause, and a missing or duplicate receipt fails validation.
type TransportReceipt = {
  id: string
  arm: {
    step: number
    kind: 'pause-pulls'
    confirmed: true
  }
  fire?: {
    step: number
    operation: Operation['kind'] | 'run-end'
    blockedPulls: number
  }
  heal?: {
    step: number
    operation: Operation['kind'] | 'run-end'
  }
}

type Operation =
  | { kind: 'desire'; slot: number; projectIDs: string[] }
  | { kind: 'undesire'; slot: number }
  | { kind: 'write'; id: string; projectID: string; rank: number }
  | { kind: 'responseLoss'; id: string; projectID: string }
  | { kind: 'prune'; epoch: number }
  | { kind: 'fullPruneRestart' }
  | { kind: 'armEngineFault'; point: FaultPoint; faultKind: FaultKind }
  | { kind: 'pausePulls' }
  | { kind: 'resumePulls' }
  | { kind: 'serverRestart' }
  | { kind: 'clientRestart' }
  | { kind: 'checkpoint' }

type RecordedOperation = Operation & {
  faultReceipt?: FaultReceipt
  transportReceipt?: TransportReceipt
}

type FaultTally = {
  armed: number
  fired: number
  notFired: number
  byFault: Record<string, { armed: number; fired: number; notFired: number }>
}

type TransportTally = {
  armed: number
  fired: number
  healed: number
}

type ExecutionReport = {
  trace: RecordedOperation[]
  faultTally: FaultTally
  transportTally: TransportTally
}

class TraceExecutionError extends Error {
  constructor(
    readonly failure: unknown,
    readonly report: ExecutionReport
  ) {
    super(String(failure), { cause: failure })
  }
}

const FAULT_POINTS: readonly FaultPoint[] = [
  'push_before_mutation',
  'push_after_write_before_commit',
  'push_after_commit_before_response',
  'pull_during_tx',
  'pull_after_commit',
]
const FAULT_KINDS: readonly FaultKind[] = ['kill', 'error', 'quota']

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'rust-local' },
    seed: { type: 'string', default: '1' },
    steps: { type: 'string', default: '24' },
    replay: { type: 'string' },
    nemesis: { type: 'boolean', default: false },
    'no-shrink': { type: 'boolean', default: false },
    'shrink-runs': { type: 'string', default: '12' },
  },
})

if (!['rust-local', 'rust-cf'].includes(args.against!))
  throw new Error('--against must be rust-local or rust-cf')
if (args.nemesis && args.against !== 'rust-local')
  throw new Error('--nemesis currently requires --against rust-local')

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
  const lifecycleRequired: Operation[] = [
    { kind: 'desire', slot: 0, projectIDs: ['p0', 'p1'] },
    { kind: 'write', id: `sm-${seed}-write`, projectID: 'p0', rank: 4.25 },
    {
      kind: 'responseLoss',
      id: `sm-${seed}-lost-response`,
      projectID: 'p1',
    },
    { kind: 'prune', epoch: 0 },
    // empty the change log to the head and reopen the same sqlite file: the
    // served cookie must not regress (mutant O1). no other system lane empties
    // the log AND restarts over the same store.
    { kind: 'fullPruneRestart' },
    { kind: 'serverRestart' },
    { kind: 'clientRestart' },
    { kind: 'desire', slot: 1, projectIDs: ['p2'] },
    { kind: 'undesire', slot: 1 },
    { kind: 'checkpoint' },
  ]
  const nemesisRequired: Operation[] = [
    { kind: 'desire', slot: 0, projectIDs: ['p0', 'p1'] },
    {
      kind: 'armEngineFault',
      point: 'push_after_commit_before_response',
      faultKind: 'error',
    },
    { kind: 'write', id: `sm-${seed}-postcommit`, projectID: 'p0', rank: 4.25 },
    { kind: 'prune', epoch: 0 },
    // composed overlap: hold a transport pause open across an engine-fault arm
    // and a server restart, so two fault classes are active at once. the engine
    // fault is canceled by the restart (a documented not-fired); the transport
    // fault heals on resume and the client must reconverge with no silent loss.
    { kind: 'pausePulls' },
    {
      kind: 'armEngineFault',
      point: 'push_after_write_before_commit',
      faultKind: 'kill',
    },
    { kind: 'serverRestart' },
    { kind: 'resumePulls' },
    // O1 at the system level, inside the composed schedule.
    { kind: 'fullPruneRestart' },
    // an independent engine fault that fires, so the run is never vacuous.
    {
      kind: 'armEngineFault',
      point: 'push_after_write_before_commit',
      faultKind: 'kill',
    },
    { kind: 'write', id: `sm-${seed}-kill`, projectID: 'p1', rank: 9.5 },
    {
      kind: 'responseLoss',
      id: `sm-${seed}-lost-response`,
      projectID: 'p1',
    },
    { kind: 'serverRestart' },
    { kind: 'clientRestart' },
    { kind: 'desire', slot: 1, projectIDs: ['p2'] },
    { kind: 'undesire', slot: 1 },
    { kind: 'checkpoint' },
  ]
  const required = args.nemesis ? nemesisRequired : lifecycleRequired
  // the prefix pairs each required arm with a firing write; truncating it
  // would generate a schedule that cannot satisfy the fired-fault gate.
  if (args.nemesis && steps < required.length)
    throw new Error(`--nemesis requires --steps >= ${required.length}`)
  const rng = mulberry32(seed)
  const generated: Operation[] = []
  const project = () => `p${Math.floor(rng() * 10)}`
  for (let index = required.length; index < steps; index++) {
    const roll = Math.floor(rng() * (args.nemesis ? 10 : 8))
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
      case 7:
        generated.push(
          args.nemesis
            ? {
                kind: 'responseLoss',
                id: `sm-${seed}-${index}-lost-response`,
                projectID: project(),
              }
            : { kind: 'checkpoint' }
        )
        break
      case 8:
      case 9: {
        const point = FAULT_POINTS[Math.floor(rng() * FAULT_POINTS.length)]!
        const faultKind =
          point === 'push_after_write_before_commit'
            ? 'kill'
            : FAULT_KINDS[Math.floor(rng() * FAULT_KINDS.length)]!
        generated.push({ kind: 'armEngineFault', point, faultKind })
        break
      }
      default:
        generated.push({ kind: 'checkpoint' })
    }
  }
  return [...required, ...generated].slice(0, steps)
}

async function startTarget(fetchImpl?: typeof fetch): Promise<StateTarget> {
  if (args.against === 'rust-local') {
    return (await import('./targets/rust-local.js')).startRustLocal({
      pullIntervalMs: 75,
      queryAware: true,
      retainChanges: args.nemesis ? 2 : 8,
      fetch: fetchImpl,
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

async function execute(trace: Operation[]): Promise<ExecutionReport> {
  const recordedTrace = trace.map((operation) => ({
    ...operation,
  })) as RecordedOperation[]
  const faultTally: FaultTally = { armed: 0, fired: 0, notFired: 0, byFault: {} }
  const transportTally: TransportTally = { armed: 0, fired: 0, healed: 0 }
  let pendingFault:
    | {
        receipt: FaultReceipt
        resolve: (status: 'fired' | 'not-fired') => void
        resolution: Promise<'fired' | 'not-fired'>
        recovery?: Promise<void>
      }
    | undefined
  // second fault class: when engaged, this client's pulls fail at the fetch
  // seam, modeling a one-client transport outage. verification is suspended
  // while paused because a paused view cannot observe server progress.
  let pullsPaused = false
  let blockedPulls = 0
  let pendingTransport: { receipt: TransportReceipt } | undefined
  let currentStep = -1
  let currentOperation: Operation = { kind: 'checkpoint' }

  const healTransport = (transport: NonNullable<typeof pendingTransport>) => {
    if (transport.receipt.heal) return
    transport.receipt.heal = {
      step: currentStep,
      operation:
        currentStep < 0 || currentStep >= recordedTrace.length
          ? 'run-end'
          : currentOperation.kind,
    }
    transportTally.healed++
    if (pendingTransport === transport) pendingTransport = undefined
  }

  const bumpFault = (receipt: FaultReceipt, field: 'armed' | 'fired' | 'notFired') => {
    faultTally[field]++
    const key = `${receipt.arm.point}/${receipt.arm.kind}`
    const entry = (faultTally.byFault[key] ??= { armed: 0, fired: 0, notFired: 0 })
    entry[field]++
  }

  const resolveFault = (
    fault: NonNullable<typeof pendingFault>,
    status: 'fired' | 'not-fired',
    reason: string
  ) => {
    if (fault.receipt.resolution) return
    fault.receipt.resolution = {
      status,
      step: currentStep,
      operation:
        currentStep < 0 || currentStep >= recordedTrace.length
          ? 'run-end'
          : currentOperation.kind,
      reason,
    }
    bumpFault(fault.receipt, status === 'fired' ? 'fired' : 'notFired')
    if (pendingFault === fault) pendingFault = undefined
    fault.resolve(status)
  }

  const onSync = (observation: SyncHttpObservation) => {
    const fault = pendingFault
    if (!fault || observation.phase !== 'terminal') return
    const pointPath = fault.receipt.arm.point.startsWith('push_') ? 'push' : 'pull'
    if (observation.path !== pointPath) return
    if (fault.receipt.arm.kind === 'kill') {
      if (observation.error !== undefined)
        resolveFault(fault, 'fired', `engine process exited during ${observation.path}`)
      return
    }
    const expectedStatus = fault.receipt.arm.kind === 'quota' ? 507 : 500
    const responseText = observation.rawResponseBody ?? ''
    if (
      observation.status === expectedStatus &&
      responseText.includes(fault.receipt.arm.point) &&
      responseText.includes('injected')
    ) {
      resolveFault(
        fault,
        'fired',
        `server confirmed injected ${observation.status} during ${observation.path}`
      )
    }
  }

  // client fetch seam: observe every push/pull for fault firing, and gate pulls
  // while the transport pause is engaged. the gate records the pause's fire on
  // the first blocked pull. the watermark probe (fullPruneRestart) uses global
  // fetch directly, so it is never gated.
  const observed = observedSyncFetch(onSync)
  const gatedFetch: typeof fetch = (input, init) => {
    const url = new URL(
      typeof input === 'string' || input instanceof URL ? input : input.url
    )
    if (pullsPaused && url.pathname.endsWith('/pull')) {
      blockedPulls++
      if (pendingTransport && !pendingTransport.receipt.fire) {
        pendingTransport.receipt.fire = {
          step: currentStep,
          operation:
            currentStep < 0 || currentStep >= recordedTrace.length
              ? 'run-end'
              : currentOperation.kind,
          blockedPulls,
        }
        transportTally.fired++
      }
      return Promise.reject(new Error('transport pause: client pull blocked'))
    }
    return observed(input, init)
  }

  const target = await startTarget(gatedFetch)
  const directory = mkdtempSync(join(tmpdir(), 'orez-state-machine-'))
  const kvStore = persistentKVStoreProvider(directory)
  const storageKey = `state-machine-${seed}`
  let client = target.createClient('state-machine-user', { kvStore, storageKey })
  const views = new Map<number, View>()

  const recoverKill = async (fault: NonNullable<typeof pendingFault>) => {
    if (fault.receipt.arm.kind !== 'kill' || fault.receipt.resolution?.status !== 'fired')
      return
    fault.recovery ??= target.restart(50)
    await fault.recovery
  }

  const recoverObservedKill = async () => {
    for (const operation of recordedTrace) {
      const receipt = operation.faultReceipt
      if (
        !receipt ||
        receipt.arm.kind !== 'kill' ||
        receipt.resolution?.status !== 'fired'
      )
        continue
      const fault = operationFaults.get(receipt.id)
      if (fault) await recoverKill(fault)
    }
  }

  const operationFaults = new Map<string, NonNullable<typeof pendingFault>>()

  const completeMutation = async (
    request: { client: Promise<unknown>; server: Promise<unknown> },
    label: string
  ) => {
    const kill =
      pendingFault?.receipt.arm.kind === 'kill' &&
      pendingFault.receipt.arm.point.startsWith('push_')
        ? pendingFault
        : undefined
    await withTimeout(request.client, `client ${label}`)
    const server = withTimeout(
      assertServerOutcome(request.server, 'success', label),
      `server ${label}`
    )
    if (kill) {
      const first = await Promise.race([
        server.then(() => 'server' as const),
        kill.resolution.then((status) => status),
      ])
      if (first === 'fired') await recoverKill(kill)
    }
    await server
  }

  let executionError: unknown

  // read the server-confirmed change-log watermark by issuing a raw null-cookie
  // pull (a snapshot's cookie is the current watermark). this is an authority
  // observation, not the client's optimistic overlay, so a regression here is a
  // real durable-cookie fault (mutant O1). the probe uses a throwaway client so
  // it never disturbs the state-machine client's cursor.
  const servedWatermark = async (label: string): Promise<bigint> => {
    if (!target.origin) throw new Error('servedWatermark requires rust-local')
    const response = await fetch(`${target.origin}/pull`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-state-machine-user',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        clientID: `wm-probe-${seed}-${label}`,
        clientGroupID: `wm-probe-group-${seed}`,
        cookie: null,
      }),
      signal: AbortSignal.timeout(10_000),
    })
    if (!response.ok)
      throw new Error(`watermark probe (${label}) pull failed ${response.status}`)
    const body = (await response.json()) as { cookie?: number | string | null }
    // the cookie is a canonical decimal string / integer; compare with BigInt so
    // an i64 above the JS safe-integer range still compares exactly.
    return BigInt(body.cookie ?? 0)
  }

  const verify = async (step: number, operation: Operation) => {
    for (const [slot, view] of views) {
      await eventually(async () => {
        await recoverObservedKill()
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
      currentStep = step
      currentOperation = operation
      await recoverObservedKill()
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
          await completeMutation(request, `write ${operation.id}`)
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
          await completeMutation(request, `lost-response ${operation.id}`)
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
          const pullKill =
            pendingFault?.receipt.arm.kind === 'kill' &&
            pendingFault.receipt.arm.point.startsWith('pull_')
              ? pendingFault
              : undefined
          for (let index = 0; index < 16; index++) {
            const id = `sm-prune-${seed}-${operation.epoch}-${index}`
            await target.sql(
              `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt") VALUES ('${id}', 'p0', '${id}', ${index}, 0, NULL, NULL)`
            )
          }
          // Make pruning self-contained so removing surrounding operations
          // during shrinking cannot create a dependency-only false failure.
          try {
            await target.pull()
          } catch (error) {
            await recoverObservedKill()
            if (pullKill?.receipt.resolution?.status !== 'fired') throw error
            await target.pull()
          }
          break
        }
        case 'fullPruneRestart': {
          if (!target.origin || !target.adminKey)
            throw new Error('fullPruneRestart requires the rust-local admin route')
          // like a serverRestart, this restarts the host and cancels any still
          // armed engine fault (a documented not-fired).
          if (pendingFault)
            resolveFault(
              pendingFault,
              'not-fired',
              'full prune + restart before fault fired'
            )
          const before = await servedWatermark(`before-${step}`)
          const response = await fetch(`${target.origin}/admin/prune-to-head`, {
            method: 'POST',
            headers: { 'x-admin-key': target.adminKey },
          })
          if (!response.ok) throw new Error(`prune-to-head failed ${response.status}`)
          await response.arrayBuffer()
          await target.restart(50)
          const after = await servedWatermark(`after-${step}`)
          if (after < before)
            throw new Error(
              `served watermark regressed across full prune + restart: ${before} -> ${after}`
            )
          break
        }
        case 'armEngineFault': {
          if (!target.origin || !target.adminKey)
            throw new Error('armEngineFault requires the rust-local admin route')
          if (pendingFault)
            resolveFault(pendingFault, 'not-fired', 'replaced by a later fault arm')
          const response = await fetch(`${target.origin}/admin/fault`, {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              'x-admin-key': target.adminKey,
            },
            body: JSON.stringify({ point: operation.point, kind: operation.faultKind }),
          })
          if (!response.ok)
            throw new Error(
              `arm ${operation.point}/${operation.faultKind} failed ${response.status}`
            )
          const body = (await response.json()) as {
            armed?: boolean
            point?: string
          }
          if (body.armed !== true || body.point !== operation.point)
            throw new Error(
              `arm ${operation.point}/${operation.faultKind} returned no receipt`
            )
          let resolve!: (status: 'fired' | 'not-fired') => void
          const receipt: FaultReceipt = {
            id: `fault-${seed}-${faultTally.armed + 1}`,
            arm: {
              step,
              point: operation.point,
              kind: operation.faultKind,
              confirmed: true,
            },
          }
          const fault = {
            receipt,
            resolve,
            resolution: new Promise<'fired' | 'not-fired'>((done) => {
              resolve = done
            }),
          }
          fault.resolve = resolve
          recordedTrace[step]!.faultReceipt = receipt
          operationFaults.set(receipt.id, fault)
          pendingFault = fault
          bumpFault(receipt, 'armed')
          break
        }
        case 'serverRestart':
          if (pendingFault)
            resolveFault(pendingFault, 'not-fired', 'server restarted before fault fired')
          await target.restart(50)
          break
        case 'pausePulls': {
          if (!target.origin)
            throw new Error('pausePulls requires the rust-local transport')
          // defensive: the required prefix never double-arms, but a shrink
          // candidate could drop the matching resume.
          if (pendingTransport) healTransport(pendingTransport)
          pullsPaused = true
          blockedPulls = 0
          const receipt: TransportReceipt = {
            id: `transport-${seed}-${transportTally.armed + 1}`,
            arm: { step, kind: 'pause-pulls', confirmed: true },
          }
          recordedTrace[step]!.transportReceipt = receipt
          pendingTransport = { receipt }
          transportTally.armed++
          // fire deterministically: drive one pull through the gate, which
          // records the pause's fire and rejects without touching the network.
          await gatedFetch(`${target.origin}/pull`, { method: 'POST' }).catch(
            () => undefined
          )
          break
        }
        case 'resumePulls': {
          pullsPaused = false
          if (pendingTransport) healTransport(pendingTransport)
          break
        }
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
      await recoverObservedKill()
      // a paused client cannot observe server progress, so convergence is only
      // checked once the transport pause heals. the resume op itself is verified.
      if (!pullsPaused) await verify(step, operation)
      if (operation.kind === 'prune') {
        await eventually(
          async () => {
            const rows = (await target.oracle(
              'SELECT floor FROM _zsync_meta WHERE lock = 1'
            )) as { floor: number | string }[]
            if (Number(rows[0]?.floor) <= 0)
              throw new Error('retention floor did not advance')
          },
          `seed ${seed} step ${step} retention pruning`,
          5_000
        )
      }
    }
  } catch (error) {
    executionError = error
  } finally {
    currentStep = trace.length
    currentOperation = { kind: 'checkpoint' }
    if (pendingFault)
      resolveFault(pendingFault, 'not-fired', 'run ended before fault fired')
    for (const view of views.values()) view.destroy()
    try {
      // target.close owns every client it created. Bound cleanup as well as the
      // operations: a broken connection must yield an artifact, not hang the
      // CI job before its always() upload step.
      await withTimeout(target.close(), 'state-machine target cleanup', 10_000).catch(
        (error) => {
          executionError ??= error
        }
      )
    } finally {
      rmSync(directory, { recursive: true, force: true })
    }
  }

  const report = { trace: recordedTrace, faultTally, transportTally }
  if (executionError) throw new TraceExecutionError(executionError, report)
  return report
}

function failureFingerprint(error: unknown) {
  const message = String(error instanceof TraceExecutionError ? error.failure : error)
  const viewFailure = message.match(
    /step \d+ (\w+): Error: slot (\d+) (diverged|is incomplete)/
  )
  if (viewFailure)
    return `view-${viewFailure[3]}:${viewFailure[1]}:slot-${viewFailure[2]}`
  if (message.includes('retention floor did not advance')) return 'retention-floor'
  if (message.includes('served watermark regressed')) return 'watermark-regression'
  if (message.includes('INVALID transport schedule')) return 'transport-schedule-invalid'
  if (message.includes('lost-response write')) return 'lost-response-cardinality'
  if (message.includes('timeout waiting for server write')) return 'server-write-timeout'
  if (message.includes('timeout waiting for client write')) return 'client-write-timeout'
  if (message.includes('timeout waiting for state-machine target cleanup'))
    return 'target-cleanup-timeout'
  return message.replaceAll(/\d+/g, '#')
}

async function minimize(trace: Operation[], expectedError: TraceExecutionError) {
  let current = trace
  let currentFailure = expectedError
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
          currentFailure = error as TraceExecutionError
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
  return { operations: current, failure: currentFailure }
}

function printFaultTally(tally: FaultTally) {
  console.log('\n[state-machine] fault tallies:')
  for (const [fault, counts] of Object.entries(tally.byFault).sort()) {
    console.log(
      `  ${fault.padEnd(48)} armed=${counts.armed} fired=${counts.fired} not-fired=${counts.notFired}`
    )
  }
  console.log(
    `[state-machine] faults armed=${tally.armed} fired=${tally.fired} not-fired=${tally.notFired}`
  )
}

function printTransportTally(tally: TransportTally) {
  if (tally.armed === 0) return
  console.log(
    `[state-machine] transport pauses armed=${tally.armed} fired=${tally.fired} healed=${tally.healed}`
  )
}

const replay = args.replay
  ? (JSON.parse(await Bun.file(args.replay).text()) as {
      trace: Operation[]
      minimized?: Operation[]
    })
  : undefined
const trace = replay?.minimized ?? replay?.trace ?? generateTrace()

console.log(
  `[state-machine] seed=${seed} target=${args.against} nemesis=${args.nemesis} operations=${trace.length}`
)
function failWithArtifact(
  failure: TraceExecutionError,
  minimized: { operations: Operation[]; failure: TraceExecutionError }
): never {
  printFaultTally(minimized.failure.report.faultTally)
  printTransportTally(minimized.failure.report.transportTally)
  const directory = join(import.meta.dirname, '..', 'regressions')
  mkdirSync(directory, { recursive: true })
  const file = join(directory, `state-machine-${args.against}-seed-${seed}.json`)
  writeFileSync(
    file,
    JSON.stringify(
      {
        seed,
        target: args.against,
        error: String(failure.failure),
        replay: `bun src/state-machine.ts --against ${args.against} --seed ${seed}${args.nemesis ? ' --nemesis' : ''} --replay ${file} --no-shrink`,
        trace: failure.report.trace,
        minimized: minimized.failure.report.trace,
        faultTally: minimized.failure.report.faultTally,
        transportTally: minimized.failure.report.transportTally,
      },
      null,
      2
    )
  )
  console.error(
    `[state-machine] minimized ${trace.length} -> ${minimized.operations.length}: ${file}`
  )
  process.exit(1)
}

let report: ExecutionReport
try {
  report = await execute(trace)
} catch (error) {
  const failure = error as TraceExecutionError
  console.error(`[state-machine] FAIL seed=${seed}: ${String(failure.failure)}`)
  failWithArtifact(
    failure,
    args['no-shrink'] ? { operations: trace, failure } : await minimize(trace, failure)
  )
}

// a generated nemesis schedule must fire at least one fault; the required
// prefix pairs each arm with a firing write, so zero fires means injection
// itself broke. replays and shrink candidates are judged on execution alone:
// receipts canceled by restart, replacement, or run-end are legitimate there,
// and shrinking a whole-run coverage property only converges on a vacuous
// trace that hides the original failure.
if (args.nemesis && !args.replay && report.faultTally.fired === 0) {
  const failure = new TraceExecutionError(
    new Error(
      `INVALID nemesis schedule: armed ${report.faultTally.armed} faults but fired none`
    ),
    report
  )
  console.error(`[state-machine] FAIL seed=${seed}: ${String(failure.failure)}`)
  failWithArtifact(failure, { operations: trace, failure })
}

// transport-pause coverage, judged only on generated schedules (like the fault
// gate above): every armed pause must fire (a pull was actually blocked) and
// heal (an explicit resume). a missing fire/heal receipt is a schedule defect.
// replays and shrink candidates are exempt: a shrink may legitimately drop the
// pause/resume pair, and that is not the failure under minimization.
if (args.nemesis && !args.replay && report.transportTally.armed > 0) {
  const t = report.transportTally
  const defect =
    t.fired < t.armed
      ? `fired ${t.fired}`
      : t.healed < t.armed
        ? `healed ${t.healed}`
        : undefined
  if (defect) {
    const failure = new TraceExecutionError(
      new Error(`INVALID transport schedule: armed ${t.armed} pauses but ${defect}`),
      report
    )
    console.error(`[state-machine] FAIL seed=${seed}: ${String(failure.failure)}`)
    failWithArtifact(failure, { operations: trace, failure })
  }
}

if (args.nemesis) {
  printFaultTally(report.faultTally)
  printTransportTally(report.transportTally)
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
      nemesis: args.nemesis,
      retainChanges: args.nemesis ? 2 : 8,
      trace: report.trace,
      faultTally: report.faultTally,
      transportTally: report.transportTally,
    },
    null,
    2
  )
)
