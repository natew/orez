import { afterEach, describe, expect, test } from 'bun:test'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { FAULT_SCHEDULE_SCHEMA_VERSION, type FaultSchedule } from './fault-schedule.js'
import {
  writePermissionArtifacts,
  type PermissionChecksArtifact,
  type PermissionManifest,
} from './permission-artifacts.js'
import { patch, validPermissionHistory } from './permission-transition.fixture.js'
import {
  checkPermissionTransition,
  classifyPermissionOutcome,
  PERMISSION_CHECKS_SCHEMA_VERSION,
  PERMISSION_HISTORY_SCHEMA_VERSION,
  PERMISSION_TRANSITION_PROFILE,
  PERMISSION_TRANSITION_PROFILE_VERSION,
  type PermissionEvent,
} from './permission-transition.js'

const tempDirs: string[] = []
afterEach(async () => {
  for (const dir of tempDirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

async function resultsDir(): Promise<string> {
  const parent = await mkdtemp(join(tmpdir(), 'perm-artifacts-'))
  tempDirs.push(parent)
  return join(parent, 'bundle')
}

function baseManifest(): PermissionManifest {
  return {
    schemaVersion: PERMISSION_CHECKS_SCHEMA_VERSION,
    kind: 'orez-permission-transition',
    runId: 'bundle',
    seed: { value: 'seed-1', source: 'fixed' },
    profile: {
      name: PERMISSION_TRANSITION_PROFILE.name,
      version: PERMISSION_TRANSITION_PROFILE_VERSION,
      historySchemaVersion: PERMISSION_HISTORY_SCHEMA_VERSION,
      checksSchemaVersion: PERMISSION_CHECKS_SCHEMA_VERSION,
    },
    host: 'http://127.0.0.1:9000',
    namespaces: { transition: 'nsA', stable: 'nsB' },
    target: { name: 'orez-local', build: 'abc1234' },
    replay: { command: 'bun src/permission-transition-lane.ts --seed=seed-1 --replay' },
  }
}

const emptySchedule = (): FaultSchedule => ({
  schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
  faultsRequired: false,
  plans: [],
  receipts: [],
})

function checksFor(history: readonly PermissionEvent[]): PermissionChecksArtifact {
  const check = checkPermissionTransition(history)
  return {
    schemaVersion: PERMISSION_CHECKS_SCHEMA_VERSION,
    kind: 'orez-permission-transition-checks',
    result: classifyPermissionOutcome(history, check),
    checks: [
      {
        name: PERMISSION_TRANSITION_PROFILE.name,
        version: String(PERMISSION_TRANSITION_PROFILE_VERSION),
        valid: check.valid,
        violations: check.violations,
      },
    ],
  }
}

async function write(overrides: {
  dir?: string
  manifest?: PermissionManifest
  history?: readonly PermissionEvent[]
  schedule?: FaultSchedule
  checks?: PermissionChecksArtifact
}): Promise<string> {
  const history = overrides.history ?? validPermissionHistory()
  const dir = overrides.dir ?? (await resultsDir())
  await writePermissionArtifacts({
    resultsDir: dir,
    manifest: overrides.manifest ?? baseManifest(),
    history,
    schedule: overrides.schedule ?? emptySchedule(),
    checks: overrides.checks ?? checksFor(history),
  })
  return dir
}

describe('permission transition artifact writer', () => {
  test('persists a validated pass bundle and reads it back', async () => {
    const history = validPermissionHistory()
    const dir = await write({ history })

    const manifest = JSON.parse(await readFile(join(dir, 'manifest.json'), 'utf8'))
    expect(manifest.profile.version).toBe(1)
    expect(manifest.profile.checksSchemaVersion).toBe(2)

    const lines = (await readFile(join(dir, 'history.jsonl'), 'utf8')).trim().split('\n')
    expect(lines).toHaveLength(history.length)

    const checks = JSON.parse(await readFile(join(dir, 'checks.json'), 'utf8'))
    expect(checks.result).toBe('pass')
    expect(checks.checks[0].valid).toBe(true)

    const schedule = JSON.parse(await readFile(join(dir, 'schedule.json'), 'utf8'))
    expect(schedule.plans).toEqual([])
  })

  test('persists a fail bundle whose recorded verdict matches the history', async () => {
    const history = patch(validPermissionHistory(), 'cAs-named-2', {
      rows: ['member:pt-member', 'project:pt-project', 'task:pt-task'],
      markers: ['mk-A'],
    })
    const dir = await write({ history })
    const checks = JSON.parse(await readFile(join(dir, 'checks.json'), 'utf8'))
    expect(checks.result).toBe('fail')
    expect(checks.checks[0].valid).toBe(false)
  })

  test('refuses to overwrite an existing results directory', async () => {
    const dir = await write({})
    await expect(write({ dir })).rejects.toThrow('refusing to overwrite')
  })

  test('rejects a manifest whose profile is not the frozen v1 profile', async () => {
    const manifest = baseManifest()
    manifest.profile.version = 2
    await expect(write({ manifest })).rejects.toThrow('frozen v1 profile')
  })

  test('rejects a manifest with empty build provenance', async () => {
    const manifest = baseManifest()
    manifest.target.build = ''
    await expect(write({ manifest })).rejects.toThrow('build provenance')
  })

  test('rejects a manifest with non-distinct namespaces', async () => {
    const manifest = baseManifest()
    manifest.namespaces.stable = manifest.namespaces.transition
    await expect(write({ manifest })).rejects.toThrow('not distinct')
  })

  test('rejects a manifest with an invalid seed source', async () => {
    const manifest = baseManifest()
    ;(manifest.seed as { source: string }).source = 'guess'
    await expect(write({ manifest })).rejects.toThrow('invalid seed source')
  })

  test('rejects a checks envelope with a wrong schema version', async () => {
    const checks = checksFor(validPermissionHistory())
    ;(checks as { schemaVersion: number }).schemaVersion = 1
    await expect(write({ checks })).rejects.toThrow('schema version 1')
  })

  test('rejects a checks envelope whose validity disagrees with its violations', async () => {
    const checks = checksFor(validPermissionHistory())
    checks.checks[0]!.valid = false
    await expect(write({ checks })).rejects.toThrow('validity disagrees')
  })

  test('rejects a result that disagrees with the persisted history', async () => {
    // an info grant is inconclusive; recording it as a fail is internally
    // consistent for validateChecks (both are "not pass") but contradicts the
    // history, which the integrity gate must catch
    const history = patch(validPermissionHistory(), 'grant', { phase: 'info' })
    const checks = checksFor(history)
    expect(checks.result).toBe('inconclusive')
    checks.result = 'fail'
    await expect(write({ history, checks })).rejects.toThrow('result does not match')
  })

  test('rejects recorded checks that do not match the persisted history', async () => {
    const history = validPermissionHistory()
    const checks = checksFor(history)
    checks.checks[0]!.violations = ['fabricated violation']
    checks.checks[0]!.valid = false
    checks.result = 'fail'
    await expect(write({ history, checks })).rejects.toThrow(
      'do not match the persisted history'
    )
  })

  test('rejects an empty history', async () => {
    await expect(write({ history: [], checks: checksFor([]) })).rejects.toThrow(
      'history is empty'
    )
  })

  test('rejects a non-empty fault schedule', async () => {
    const schedule: FaultSchedule = {
      schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
      faultsRequired: false,
      plans: [
        {
          id: 'p',
          kind: 'k',
          arm: { logicalStep: 0, hook: 'a' },
          fire: { logicalStep: 1, hook: 'b' },
        },
      ],
      receipts: [
        { planId: 'p', phase: 'arm', logicalStep: 0, hook: 'a' },
        { planId: 'p', phase: 'fire', logicalStep: 1, hook: 'b' },
      ],
    }
    await expect(write({ schedule })).rejects.toThrow('empty fault schedule only')
  })
})
