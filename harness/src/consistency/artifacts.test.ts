import { afterEach, describe, expect, test } from 'bun:test'
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises'
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

function completeRecorder(): HistoryRecorder {
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
