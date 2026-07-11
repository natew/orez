// One-group orez-local reliability lane for the dedicated lost-push/LMID
// profile. This proves one non-idempotent authoritative application and one
// fresh-client LMID advance through the empirically observed recovery path.
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
  checkExactlyOnceLmid,
  EXACTLY_ONCE_LMID_PROFILE,
} from './consistency/exactly-once-lmid.js'
import {
  assertExpectedExactlyOncePush,
  parseExactlyOncePush,
} from './consistency/exactly-once-workload.js'
import {
  FAULT_SCHEDULE_SCHEMA_VERSION,
  type FaultReceipt,
  type FaultSchedule,
} from './consistency/fault-schedule.js'
import {
  HISTORY_SCHEMA_VERSION,
  type ExactlyOnceEvidence,
  type ExactlyOnceIdentity,
  type HistoryKind,
} from './consistency/history.js'
import { HistoryRecorder } from './consistency/recorder.js'
import { mutators, queries } from './fixture.js'
import {
  createOperationBoundDropFetch,
  observedSyncFetch,
  type SyncHttpObservation,
} from './observed-fetch.js'
import { assertServerOutcome } from './server-outcome.js'
import { startOrezLocal } from './targets/orez-local.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'orez-local' },
    seed: { type: 'string' },
    replay: { type: 'boolean', default: false },
    'results-dir': { type: 'string' },
  },
})
if (args.target !== 'orez-local') throw new Error('v1 supports only orez-local')
const seed = args.seed ?? randomUUID()
if (!/^[A-Za-z0-9._:-]+$/.test(seed)) throw new Error('seed has unsafe characters')
const scenario = `exactly-once-${createHash('sha256').update(seed).digest('hex').slice(0, 16)}`
const probeId = `${scenario}-probe`
const defaultRunId = args.replay
  ? `${scenario}-replay-${randomUUID().slice(0, 8)}`
  : scenario
const resultsDir =
  args['results-dir'] ?? join('target', 'consistency', 'exactly-once-lmid', defaultRunId)
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
if (dirty)
  throw new Error(`refusing evidence run with dirty executable inputs:\n${dirty}`)
const build = execFileSync('git', ['rev-parse', 'HEAD'], {
  cwd: repoRoot,
  encoding: 'utf8',
}).trim()

const recorder = new HistoryRecorder(() => Math.floor(performance.now() * 1_000))
let identity: ExactlyOnceIdentity | undefined
let recordingProtocol = false
let protocolError: unknown
let pushAttempt = 0
let pullAttempt = 0
let pushTerminals = 0
let pullTerminals = 0
let capturedPushBody: string | undefined
type PushSource = 'stock-client' | 'harness-replay'
const protocolOps = new Map<string, { opId: string; evidence: ExactlyOnceEvidence }>()
const ignoredProtocolRequests = new Set<string>()
const protocolWaiters = new Set<() => void>()

function recordPair(
  opId: string,
  process: string,
  kind: HistoryKind,
  invoke: ExactlyOnceEvidence,
  terminal: ExactlyOnceEvidence,
  phase: 'ok' | 'fail' | 'info' = 'ok'
) {
  recorder.record({
    opId,
    process,
    phase: 'invoke',
    kind,
    clientId: invoke.identity.clientId,
    clientGroupId: invoke.identity.clientGroupId,
    exactlyOnce: invoke,
  })
  return recorder.record({
    opId,
    process,
    phase,
    kind,
    clientId: terminal.identity.clientId,
    clientGroupId: terminal.identity.clientGroupId,
    exactlyOnce: terminal,
  })
}

function protocolObservation(source: PushSource, observation: SyncHttpObservation): void {
  if (!recordingProtocol || !identity) return
  try {
    if (observation.phase === 'invoke') {
      if (observation.path === 'push') {
        const parsed = parseExactlyOncePush(observation.body)
        if (typeof observation.rawBody !== 'string') {
          throw new Error('push request body is not an exact string')
        }
        assertExpectedExactlyOncePush(parsed, { identity, args: { id: probeId } })
        const attempt = ++pushAttempt
        if (attempt > 2) throw new Error(`unexpected push attempt ${attempt}`)
        if (attempt === 1 && source === 'stock-client') {
          capturedPushBody = observation.rawBody
        }
        const evidence = {
          type: 'push' as const,
          profileVersion: 1 as const,
          identity,
          attempt,
          source,
          bodyDigest: parsed.bodyDigest,
          rawBodySha256: createHash('sha256').update(observation.rawBody).digest('hex'),
          observed: null,
        }
        const opId = `${runId}-push-${attempt}`
        recorder.record({
          opId,
          process: `push-${attempt}`,
          phase: 'invoke',
          kind: 'push',
          clientId: identity.clientId,
          clientGroupId: identity.clientGroupId,
          exactlyOnce: evidence,
        })
        protocolOps.set(`${source}:${observation.request}`, { opId, evidence })
      } else {
        if (source !== 'stock-client') {
          throw new Error('harness replay observer received pull traffic')
        }
        const body = observation.body as Record<string, unknown>
        if (
          body?.clientID !== identity.clientId ||
          body?.clientGroupID !== identity.clientGroupId
        ) {
          throw new Error('pull identity does not match the fresh client')
        }
        const attempt = ++pullAttempt
        // The frozen empirical path includes a second pull invocation that had
        // not completed when request.server settled. Keep incomplete protocol
        // traffic outside the paired consistency history.
        if (attempt > 1) {
          ignoredProtocolRequests.add(`${source}:${observation.request}`)
          return
        }
        const evidence = {
          type: 'pull' as const,
          profileVersion: 1 as const,
          identity: {
            clientId: identity.clientId,
            clientGroupId: identity.clientGroupId,
          },
          attempt,
          observed: null,
        }
        const opId = `${runId}-pull-${attempt}`
        recorder.record({
          opId,
          process: `pull-${attempt}`,
          phase: 'invoke',
          kind: 'pull',
          clientId: identity.clientId,
          clientGroupId: identity.clientGroupId,
          exactlyOnce: evidence,
        })
        protocolOps.set(`${source}:${observation.request}`, { opId, evidence })
      }
      return
    }

    const requestKey = `${source}:${observation.request}`
    if (ignoredProtocolRequests.delete(requestKey)) return
    const pending = protocolOps.get(requestKey)
    if (!pending) throw new Error(`protocol request ${observation.request} has no invoke`)
    if (pending.evidence.type === 'push') {
      const response = observation.response as {
        pushResponse?: {
          mutations?: Array<{
            id?: { clientID?: string; id?: number }
            result?: { error?: string; details?: string }
          }>
        }
      }
      const mutation = response?.pushResponse?.mutations?.[0]
      const observed = observation.error
        ? ({ outcome: 'response-lost' } as const)
        : typeof observation.rawResponseBody === 'string' &&
            observation.response !== undefined
          ? ({
              outcome: 'response',
              status: observation.status ?? 0,
              bodySha256: createHash('sha256')
                .update(observation.rawResponseBody)
                .digest('hex'),
              responseClientId: mutation?.id?.clientID ?? null,
              responseMutationCount: response?.pushResponse?.mutations?.length ?? 0,
              mutationId: mutation?.id?.id ?? null,
              error: mutation?.result?.error ?? null,
              details: mutation?.result?.details ?? null,
            } as const)
          : (() => {
              throw new Error('push terminal response is not valid JSON evidence')
            })()
      recorder.record({
        opId: pending.opId,
        process: `push-${pending.evidence.attempt}`,
        phase: observed.outcome === 'response-lost' ? 'info' : 'ok',
        kind: 'push',
        clientId: identity.clientId,
        clientGroupId: identity.clientGroupId,
        exactlyOnce: { ...pending.evidence, observed },
      })
      pushTerminals++
    } else if (pending.evidence.type === 'pull') {
      const changes = (observation.response as { lastMutationIDChanges?: unknown })
        ?.lastMutationIDChanges as Record<string, unknown> | undefined
      const lmid = changes?.[identity.clientId]
      recorder.record({
        opId: pending.opId,
        process: `pull-${pending.evidence.attempt}`,
        phase: 'ok',
        kind: 'pull',
        clientId: identity.clientId,
        clientGroupId: identity.clientGroupId,
        exactlyOnce: {
          ...pending.evidence,
          observed: {
            outcome: 'pull-lmid-observed',
            lastMutationId: lmid === undefined ? null : String(lmid),
          },
        },
      })
      pullTerminals++
    }
    protocolOps.delete(requestKey)
    for (const wake of protocolWaiters) wake()
  } catch (error) {
    protocolError = error
    for (const wake of protocolWaiters) wake()
  }
}

async function waitForProtocol(expectedPushes: number): Promise<void> {
  const complete = () => {
    if (protocolError) throw protocolError
    if (pushTerminals !== expectedPushes || pullTerminals !== 1) {
      throw new Error(
        `waiting for two push/one pull terminals, got ${pushTerminals}/${pullTerminals}`
      )
    }
  }
  let timer: ReturnType<typeof setTimeout> | undefined
  await new Promise<void>((resolve, reject) => {
    const inspect = () => {
      try {
        complete()
        protocolWaiters.delete(inspect)
        clearTimeout(timer)
        resolve()
      } catch (error) {
        if (protocolError || pushTerminals > expectedPushes || pullTerminals > 1) {
          protocolWaiters.delete(inspect)
          clearTimeout(timer)
          reject(error)
        }
      }
    }
    protocolWaiters.add(inspect)
    timer = setTimeout(() => {
      protocolWaiters.delete(inspect)
      reject(
        new Error(
          `timed out waiting for exact recovery traffic (push invokes/terminals ${pushAttempt}/${pushTerminals}, pull invokes/terminals ${pullAttempt}/${pullTerminals}, pending ${protocolOps.size}, error ${String(protocolError)})`
        )
      )
    }, 30_000)
    inspect()
  })
}

async function withTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), 30_000)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

let targetForDropConsume: ReturnType<typeof startOrezLocal> extends Promise<infer T>
  ? T
  : never
const dropFetch = createOperationBoundDropFetch((token) =>
  targetForDropConsume.consumeExactlyOnceResponseDrop(token)
)
const stockFetch = observedSyncFetch(
  (observation) => protocolObservation('stock-client', observation),
  dropFetch.fetch
)
const harnessReplayFetch = observedSyncFetch((observation) =>
  protocolObservation('harness-replay', observation)
)
const target = await startOrezLocal({
  pullIntervalMs: 0,
  fetch: stockFetch,
})
targetForDropConsume = target
let client: ReturnType<typeof target.createClient> | undefined
const replay = `bun src/exactly-once-lmid-lane.ts --target orez-local --seed=${seed} --replay`
try {
  await target.sql(
    `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt") VALUES ('${probeId}', 'p0', 'exactly-once probe', 0, 0, NULL, NULL)`
  )
  client = target.createClient('exactly-once-client')
  identity = {
    clientId: client.clientID,
    clientGroupId: await client.clientGroupID,
    mutationId: 1,
  }
  const effect = { type: 'increment-probe' as const, probeId }
  const authority = async (observation: 'before' | 'after') => {
    const stable = {
      type: 'authority' as const,
      profileVersion: 1 as const,
      observation,
      identity: identity!,
      effect,
    }
    const opId = `${runId}-authority-${observation}`
    recorder.record({
      opId,
      process: `authority-${observation}`,
      phase: 'invoke',
      kind: 'read',
      clientId: identity!.clientId,
      clientGroupId: identity!.clientGroupId,
      exactlyOnce: { ...stable, observed: null },
    })
    const probes = await target.oracle(`SELECT rank FROM task WHERE id = '${probeId}'`)
    const clients = await target.oracle(
      `SELECT lastMutationID FROM _zsync_clients WHERE clientGroupID = '${identity!.clientGroupId}' AND clientID = '${identity!.clientId}'`
    )
    return recorder.record({
      opId,
      process: `authority-${observation}`,
      phase: 'ok',
      kind: 'read',
      clientId: identity!.clientId,
      clientGroupId: identity!.clientGroupId,
      exactlyOnce: {
        ...stable,
        observed: {
          probeRowCount: probes.length,
          applicationCount: probes.length === 1 ? String(probes[0]!.rank) : '0',
          clientRowCount: clients.length,
          lastMutationId:
            clients.length === 1 ? String(clients[0]!.lastMutationID) : null,
        },
      },
    })
  }
  await authority('before')

  const clientProbeStable = {
    type: 'client-probe' as const,
    profileVersion: 1 as const,
    identity,
    effect,
  }
  const clientProbeOp = `${runId}-client-probe`
  recorder.record({
    opId: clientProbeOp,
    process: 'client-probe',
    phase: 'invoke',
    kind: 'read',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: { ...clientProbeStable, observed: null },
  })
  const probeView = client.materialize(queries.taskById({ id: probeId }))
  await new Promise<void>((resolve, reject) => {
    const deadline = setTimeout(
      () => reject(new Error('timed out hydrating the rank-0 probe')),
      30_000
    )
    probeView.addListener((data, resultType) => {
      if (resultType !== 'complete') return
      clearTimeout(deadline)
      if (!data || data.rank !== 0) {
        reject(new Error(`initial client probe rank is ${String(data?.rank)}`))
      } else {
        recorder.record({
          opId: clientProbeOp,
          process: 'client-probe',
          phase: 'ok',
          kind: 'read',
          clientId: identity!.clientId,
          clientGroupId: identity!.clientGroupId,
          exactlyOnce: {
            ...clientProbeStable,
            observed: { resultType: 'complete', applicationCount: String(data.rank) },
          },
        })
        resolve()
      }
    })
  })
  probeView.destroy()

  const mutationOp = `${runId}-mutation`
  const planId = `${runId}-drop-response`
  const hooks = {
    arm: 'before-push',
    fire: 'after-commit-before-client-delivery',
    heal: 'response-drop-consumed',
  } as const
  const receipts: FaultReceipt[] = []
  const faultStage = (stage: 'arm' | 'fire' | 'heal') => {
    const stable = {
      type: 'fault' as const,
      profileVersion: 1 as const,
      identity: identity!,
      planId,
      operationId: mutationOp,
      stage,
      hook: hooks[stage],
    }
    const event = recordPair(
      `${runId}-fault-${stage}`,
      `fault-${stage}`,
      'fault',
      { ...stable, observed: null },
      { ...stable, observed: { acknowledged: true } }
    )
    receipts.push({
      planId,
      phase: stage,
      logicalStep: stage === 'arm' ? 1 : stage === 'fire' ? 2 : 3,
      hook: hooks[stage],
      operationId: mutationOp,
      identity: identity!,
      anchor: { historyIndex: event.index, historyOpId: event.opId },
    })
  }
  const dropToken = target.armExactlyOnceResponseDrop(
    { identity, args: { id: probeId } },
    faultStage
  )
  dropFetch.arm(dropToken)
  recordingProtocol = true

  const mutationEvidence = {
    type: 'mutation' as const,
    profileVersion: 1 as const,
    identity,
    effect,
  }
  recorder.record({
    opId: mutationOp,
    process: 'writer',
    phase: 'invoke',
    kind: 'mutation',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: mutationEvidence,
  })
  const request = client.mutate(mutators.exactlyOnce.incrementProbe({ id: probeId }))
  let mutationPhase: 'ok' | 'fail' | 'info' = 'ok'
  let mutationError: string | undefined
  try {
    await withTimeout(
      assertServerOutcome(request.server, 'success', mutationOp),
      'mutation server outcome'
    )
  } catch (error) {
    mutationError = error instanceof Error ? error.message : String(error)
    mutationPhase = mutationError.includes('timed out') ? 'info' : 'fail'
  }
  recorder.record({
    opId: mutationOp,
    process: 'writer',
    phase: mutationPhase,
    kind: 'mutation',
    clientId: identity.clientId,
    clientGroupId: identity.clientGroupId,
    exactlyOnce: mutationEvidence,
    ...(mutationError ? { error: mutationError } : {}),
  })
  await waitForProtocol(1)
  if (mutationPhase !== 'fail') {
    if (!capturedPushBody) throw new Error('stock push raw body was not captured')
    const replayResponse = await harnessReplayFetch(`${target.origin}/push`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-exactly-once-client',
        'content-type': 'application/json',
      },
      body: capturedPushBody,
    })
    await replayResponse.arrayBuffer()
    await waitForProtocol(2)
  }
  recordingProtocol = false
  await authority('after')

  const schedule: FaultSchedule = {
    schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
    faultsRequired: true,
    plans: [
      {
        id: planId,
        kind: 'drop-push-response',
        arm: { logicalStep: 1, hook: hooks.arm },
        fire: { logicalStep: 2, hook: hooks.fire },
        heal: { logicalStep: 3, hook: hooks.heal },
        operationId: mutationOp,
        identity,
      },
    ],
    receipts,
  }
  const outcome = checkExactlyOnceLmid(recorder.snapshot(), schedule)
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
        name: EXACTLY_ONCE_LMID_PROFILE.name,
        version: EXACTLY_ONCE_LMID_PROFILE.version,
      },
      target: { name: target.name, build },
      replay: { command: replay, env: {} },
    },
    schedule,
    checks: {
      schemaVersion: CHECKS_SCHEMA_VERSION,
      kind: 'orez-consistency-checks',
      checks: [
        {
          name: 'exactly-once-lmid',
          version: String(EXACTLY_ONCE_LMID_PROFILE.version),
          inputs: ['history.jsonl', 'schedule.json'],
          ...outcome,
        },
      ],
    },
  })
  console.log(`[exactly-once-lmid] ${outcome.status.toUpperCase()} ${resultsDir}`)
  console.log(`[exactly-once-lmid] replay: ${replay}`)
  if (outcome.status !== 'pass') {
    throw new Error(
      outcome.status === 'fail'
        ? outcome.violations.join('\n')
        : outcome.reports?.join('\n')
    )
  }
} catch (error) {
  console.error(`[exactly-once-lmid] seed: ${seed}`)
  console.error(`[exactly-once-lmid] replay: ${replay}`)
  console.error(`[exactly-once-lmid] failure: ${String(error)}`)
  throw error
} finally {
  recordingProtocol = false
  await client?.close().catch(() => {})
  await target.close()
}
