import { afterEach, describe, expect, test } from 'bun:test'
import { mkdtemp, readdir, readFile, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { writeConsistencyArtifacts, type ConsistencyChecksArtifact } from './artifacts.js'
import { FAULT_SCHEDULE_SCHEMA_VERSION, type FaultSchedule } from './fault-schedule.js'
import {
  HISTORY_SCHEMA_VERSION,
  validateHistory,
  type HistoryEvent,
  type RunManifest,
} from './history.js'
import { HistoryRecorder } from './recorder.js'

const tempDirs: string[] = []

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((path) => rm(path, { recursive: true })))
})

async function tempResults(): Promise<{ parent: string; results: string }> {
  const parent = await mkdtemp(join(tmpdir(), 'orez-consistency-'))
  tempDirs.push(parent)
  return { parent, results: join(parent, 'run-1') }
}

function manifest(): RunManifest {
  return {
    schemaVersion: HISTORY_SCHEMA_VERSION,
    kind: 'orez-consistency-history',
    runId: 'run-1',
    seed: { value: '424242', source: 'fixed' },
    workload: { name: 'artifact-self-test', version: 1 },
    target: { name: 'pure-recorder', build: 'test-build' },
    replay: {
      command: 'bun harness/src/consistency/replay.ts --run run-1',
      env: { OREZ_SEED: '424242' },
    },
  }
}

function schedule(): FaultSchedule {
  return {
    schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
    faultsRequired: true,
    plans: [
      {
        id: 'fault-1',
        kind: 'drop-response',
        arm: { logicalStep: 1, hook: 'before-push' },
        fire: { logicalStep: 2, hook: 'after-commit' },
        heal: { logicalStep: 3, hook: 'before-replay' },
      },
    ],
    receipts: [
      { planId: 'fault-1', phase: 'arm', logicalStep: 1, hook: 'before-push' },
      { planId: 'fault-1', phase: 'fire', logicalStep: 2, hook: 'after-commit' },
      { planId: 'fault-1', phase: 'heal', logicalStep: 3, hook: 'before-replay' },
    ],
  }
}

function checks(): ConsistencyChecksArtifact {
  return {
    schemaVersion: HISTORY_SCHEMA_VERSION,
    kind: 'orez-consistency-checks',
    checks: [
      {
        name: 'history-structure',
        version: '1',
        input: 'history.jsonl',
        valid: true,
        violations: [],
      },
    ],
  }
}

function completeRecorder(metadata?: Record<string, unknown>): HistoryRecorder {
  const times = [10_000, 10_005]
  const recorder = new HistoryRecorder(() => times.shift()!)
  recorder.record({
    opId: 'read-1',
    process: 'reader-1',
    phase: 'invoke',
    kind: 'read',
    clientId: 'client-1',
  })
  recorder.record({
    opId: 'read-1',
    process: 'reader-1',
    phase: 'ok',
    kind: 'read',
    clientId: 'client-1',
    snapshot: { generation: 'generation-1', watermark: '9007199254740993' },
    metadata,
  })
  return recorder
}

async function exists(path: string): Promise<boolean> {
  try {
    await stat(path)
    return true
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false
    throw error
  }
}

describe('consistency artifact writer', () => {
  test('writes four schema-v1 artifacts that round-trip through validation', async () => {
    const { results } = await tempResults()
    await writeConsistencyArtifacts({
      resultsDir: results,
      recorder: completeRecorder(),
      manifest: manifest(),
      schedule: schedule(),
      checks: checks(),
    })

    const names = ['manifest.json', 'history.jsonl', 'schedule.json', 'checks.json']
    for (const name of names) expect(await exists(join(results, name))).toBe(true)

    const savedManifest = JSON.parse(
      await readFile(join(results, 'manifest.json'), 'utf8')
    )
    expect(savedManifest.replay).toEqual(manifest().replay)
    const history = (await readFile(join(results, 'history.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as HistoryEvent)
    expect(validateHistory(history)).toEqual({ valid: true, violations: [] })
    expect(history[1]!.snapshot!.watermark).toBe('9007199254740993')
    expect(typeof history[1]!.snapshot!.watermark).toBe('string')
    expect(await readdir(join(results, '..'))).toEqual(['run-1'])
  })

  test('refuses to overwrite an existing results directory', async () => {
    const { results } = await tempResults()
    await writeConsistencyArtifacts({
      resultsDir: results,
      recorder: completeRecorder(),
      manifest: manifest(),
      schedule: schedule(),
      checks: checks(),
    })
    await expect(
      writeConsistencyArtifacts({
        resultsDir: results,
        recorder: completeRecorder(),
        manifest: manifest(),
        schedule: schedule(),
        checks: checks(),
      })
    ).rejects.toThrow(`refusing to overwrite results directory ${results}`)
  })

  test('refuses incomplete finalize without creating the results directory', async () => {
    const { results } = await tempResults()
    const recorder = new HistoryRecorder(() => 0)
    recorder.record({
      opId: 'pending',
      process: 'client-1',
      phase: 'invoke',
      kind: 'mutation',
    })
    await expect(
      writeConsistencyArtifacts({
        resultsDir: results,
        recorder,
        manifest: manifest(),
        schedule: schedule(),
        checks: checks(),
      })
    ).rejects.toThrow('cannot finalize history with pending operations: pending')
    expect(await exists(results)).toBe(false)
  })

  test('rejects schema mutants before creating artifacts', async () => {
    const { results } = await tempResults()
    const badSchedule = schedule()
    badSchedule.receipts.pop()
    await expect(
      writeConsistencyArtifacts({
        resultsDir: results,
        recorder: completeRecorder(),
        manifest: manifest(),
        schedule: badSchedule,
        checks: checks(),
      })
    ).rejects.toThrow('plan fault-1 expected exactly one heal receipt, got 0')
    expect(await exists(results)).toBe(false)
  })

  test('serialization failure leaves no final or staging bundle', async () => {
    const { parent, results } = await tempResults()
    await expect(
      writeConsistencyArtifacts({
        resultsDir: results,
        recorder: completeRecorder({ bad: 1n }),
        manifest: manifest(),
        schedule: schedule(),
        checks: checks(),
      })
    ).rejects.toThrow('BigInt')
    expect(await exists(results)).toBe(false)
    expect(await readdir(parent)).toEqual([])
  })

  test('rejects malformed manifest metadata before creating artifacts', async () => {
    const cases: [string, (value: RunManifest) => void][] = [
      ['manifest has an empty seed value', (value) => (value.seed.value = '')],
      [
        'manifest has invalid seed source other',
        (value) => (value.seed.source = 'other' as never),
      ],
      ['manifest has an empty workload name', (value) => (value.workload.name = '')],
      [
        'manifest has invalid workload version 0',
        (value) => (value.workload.version = 0),
      ],
      [
        `manifest has invalid workload version ${Number.MAX_SAFE_INTEGER + 1}`,
        (value) => (value.workload.version = Number.MAX_SAFE_INTEGER + 1),
      ],
      ['manifest has an empty target name', (value) => (value.target.name = '')],
      ['manifest has an empty target build', (value) => (value.target.build = '')],
      [
        'manifest has invalid replay environment key BAD-KEY',
        (value) => (value.replay.env['BAD-KEY'] = 'value'),
      ],
      [
        'manifest has invalid replay environment value for BAD_VALUE',
        (value) => (value.replay.env.BAD_VALUE = 1 as never),
      ],
    ]
    for (const [message, mutate] of cases) {
      const { results } = await tempResults()
      const candidate = manifest()
      mutate(candidate)
      await expect(
        writeConsistencyArtifacts({
          resultsDir: results,
          recorder: completeRecorder(),
          manifest: candidate,
          schedule: schedule(),
          checks: checks(),
        })
      ).rejects.toThrow(message)
      expect(await exists(results)).toBe(false)
    }
  })

  test('rejects malformed or ambiguous checks before creating artifacts', async () => {
    const cases: [string, (value: ConsistencyChecksArtifact) => void][] = [
      ['checks artifact has no checks', (value) => (value.checks = [])],
      [
        'check identity history-structure@1 is not unique',
        (value) => value.checks.push(structuredClone(value.checks[0]!)),
      ],
      [
        'check history-structure has invalid input other.json',
        (value) => (value.checks[0]!.input = 'other.json' as never),
      ],
      [
        'check history-structure has an empty violation',
        (value) => {
          value.checks[0]!.valid = false
          value.checks[0]!.violations = ['']
        },
      ],
      [
        'check history-structure has malformed reports',
        (value) => (value.checks[0]!.reports = ['']),
      ],
    ]
    for (const [message, mutate] of cases) {
      const { results } = await tempResults()
      const candidate = checks()
      mutate(candidate)
      await expect(
        writeConsistencyArtifacts({
          resultsDir: results,
          recorder: completeRecorder(),
          manifest: manifest(),
          schedule: schedule(),
          checks: candidate,
        })
      ).rejects.toThrow(message)
      expect(await exists(results)).toBe(false)
    }
  })

  test('saved history mutants are rejected by validateHistory', async () => {
    const { results } = await tempResults()
    await writeConsistencyArtifacts({
      resultsDir: results,
      recorder: completeRecorder(),
      manifest: manifest(),
      schedule: schedule(),
      checks: checks(),
    })
    const history = (await readFile(join(results, 'history.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as HistoryEvent)
    history[1]!.index = 9
    expect(validateHistory(history).violations).toEqual(['event 1 has index 9'])
  })
})
