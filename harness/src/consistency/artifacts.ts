import { mkdir, mkdtemp, rename, rm, stat, writeFile } from 'node:fs/promises'
import { basename, dirname, join } from 'node:path'

import { validateFaultSchedule, type FaultSchedule } from './fault-schedule.js'
import { HISTORY_SCHEMA_VERSION, validateHistory, type RunManifest } from './history.js'
import { assertLosslessJsonValue } from './json-value.js'
import { HistoryRecorder } from './recorder.js'

export type ConsistencyCheck = {
  name: string
  version: string
  inputs: ('history.jsonl' | 'schedule.json')[]
  status: 'pass' | 'fail' | 'inconclusive'
  violations: string[]
  reports?: string[]
}

export type ConsistencyChecksArtifact = {
  schemaVersion: typeof CHECKS_SCHEMA_VERSION
  kind: 'orez-consistency-checks'
  checks: ConsistencyCheck[]
}

export const CHECKS_SCHEMA_VERSION = 2 as const

export type WriteConsistencyArtifactsOptions = {
  resultsDir: string
  recorder: HistoryRecorder
  manifest: RunManifest
  schedule: FaultSchedule
  checks: ConsistencyChecksArtifact
}

function validateManifest(manifest: RunManifest): void {
  if (manifest.schemaVersion !== HISTORY_SCHEMA_VERSION) {
    throw new Error(`manifest has schema version ${manifest.schemaVersion}`)
  }
  if (manifest.kind !== 'orez-consistency-history') {
    throw new Error(`manifest has kind ${String(manifest.kind)}`)
  }
  if (typeof manifest.runId !== 'string' || manifest.runId.trim() === '') {
    throw new Error('manifest has an empty runId')
  }
  if (typeof manifest.seed?.value !== 'string' || manifest.seed.value.trim() === '') {
    throw new Error('manifest has an empty seed value')
  }
  if (!['fixed', 'random', 'replay'].includes(manifest.seed.source)) {
    throw new Error(`manifest has invalid seed source ${String(manifest.seed.source)}`)
  }
  if (
    typeof manifest.workload?.name !== 'string' ||
    manifest.workload.name.trim() === ''
  ) {
    throw new Error('manifest has an empty workload name')
  }
  if (
    !Number.isSafeInteger(manifest.workload.version) ||
    manifest.workload.version <= 0
  ) {
    throw new Error(`manifest has invalid workload version ${manifest.workload.version}`)
  }
  if (typeof manifest.target?.name !== 'string' || manifest.target.name.trim() === '') {
    throw new Error('manifest has an empty target name')
  }
  if (typeof manifest.target?.build !== 'string' || manifest.target.build.trim() === '') {
    throw new Error('manifest has an empty target build')
  }
  if (
    typeof manifest.replay?.command !== 'string' ||
    manifest.replay.command.trim() === ''
  ) {
    throw new Error('manifest has an empty replay command')
  }
  if (
    manifest.replay.env === null ||
    typeof manifest.replay.env !== 'object' ||
    Array.isArray(manifest.replay.env)
  ) {
    throw new Error('manifest has invalid replay environment')
  }
  for (const [key, value] of Object.entries(manifest.replay.env)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
      throw new Error(`manifest has invalid replay environment key ${key}`)
    }
    if (typeof value !== 'string' || value.includes('\u0000')) {
      throw new Error(`manifest has invalid replay environment value for ${key}`)
    }
  }
}

function validateChecks(checks: ConsistencyChecksArtifact): void {
  if (checks.schemaVersion !== CHECKS_SCHEMA_VERSION) {
    throw new Error(`checks have schema version ${checks.schemaVersion}`)
  }
  if (checks.kind !== 'orez-consistency-checks') {
    throw new Error(`checks have kind ${String(checks.kind)}`)
  }
  if (!Array.isArray(checks.checks) || checks.checks.length === 0) {
    throw new Error('checks artifact has no checks')
  }
  const identities = new Set<string>()
  for (const [index, check] of checks.checks.entries()) {
    if (typeof check.name !== 'string' || check.name.trim() === '') {
      throw new Error(`check ${index} has an empty name`)
    }
    if (check.name.includes('\u0000')) throw new Error(`check ${index} name contains NUL`)
    if (typeof check.version !== 'string' || check.version.trim() === '') {
      throw new Error(`check ${index} has an empty version`)
    }
    if (check.version.includes('\u0000')) {
      throw new Error(`check ${check.name} version contains NUL`)
    }
    const identity = `${check.name}\u0000${check.version}`
    if (identities.has(identity)) {
      throw new Error(`check identity ${check.name}@${check.version} is not unique`)
    }
    identities.add(identity)
    if (
      !Array.isArray(check.inputs) ||
      check.inputs.length === 0 ||
      check.inputs.some(
        (input) => input !== 'history.jsonl' && input !== 'schedule.json'
      ) ||
      new Set(check.inputs).size !== check.inputs.length
    ) {
      throw new Error(`check ${check.name} has invalid inputs`)
    }
    if (!Array.isArray(check.violations)) {
      throw new Error(`check ${check.name} has malformed violations`)
    }
    if (
      check.violations.some(
        (violation) => typeof violation !== 'string' || violation.trim() === ''
      )
    ) {
      throw new Error(`check ${check.name} has an empty violation`)
    }
    if (
      check.reports !== undefined &&
      (!Array.isArray(check.reports) ||
        check.reports.some(
          (report) => typeof report !== 'string' || report.trim() === ''
        ))
    ) {
      throw new Error(`check ${check.name} has malformed reports`)
    }
    if (!['pass', 'fail', 'inconclusive'].includes(check.status)) {
      throw new Error(`check ${check.name} has invalid status ${String(check.status)}`)
    }
    if (check.status === 'pass' && check.violations.length !== 0) {
      throw new Error(`passing check ${check.name} has violations`)
    }
    if (check.status === 'fail' && check.violations.length === 0) {
      throw new Error(`failing check ${check.name} has no violations`)
    }
    if (
      check.status === 'inconclusive' &&
      (check.violations.length !== 0 || (check.reports?.length ?? 0) === 0)
    ) {
      throw new Error(`inconclusive check ${check.name} has invalid evidence`)
    }
  }
}

async function refuseExistingDirectory(resultsDir: string): Promise<void> {
  try {
    await stat(resultsDir)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return
    throw error
  }
  throw new Error(`refusing to overwrite results directory ${resultsDir}`)
}

export async function writeConsistencyArtifacts(
  options: WriteConsistencyArtifactsOptions
): Promise<void> {
  const { resultsDir, recorder, manifest, schedule, checks } = options
  await refuseExistingDirectory(resultsDir)
  validateManifest(manifest)
  validateChecks(checks)
  const scheduleResult = validateFaultSchedule(schedule)
  if (!scheduleResult.valid) {
    throw new Error(
      `cannot write invalid schedule:\n${scheduleResult.violations.join('\n')}`
    )
  }
  const history = recorder.finalize()
  const historyResult = validateHistory(history)
  if (!historyResult.valid) {
    throw new Error(
      `cannot write invalid history:\n${historyResult.violations.join('\n')}`
    )
  }

  assertLosslessJsonValue(manifest, 'manifest')
  assertLosslessJsonValue(history, 'history')
  assertLosslessJsonValue(schedule, 'schedule')
  assertLosslessJsonValue(checks, 'checks')

  // Serialize before reserving any filesystem path. A BigInt or other invalid
  // runtime payload must not strand an empty final directory.
  const json = (value: unknown) => `${JSON.stringify(value, null, 2)}\n`
  const serialized = {
    manifest: json(manifest),
    history: history.map((event) => JSON.stringify(event)).join('\n') + '\n',
    schedule: json(schedule),
    checks: json(checks),
  }

  const parent = dirname(resultsDir)
  await mkdir(parent, { recursive: true })
  const staging = await mkdtemp(join(parent, `.${basename(resultsDir)}.writing-`))
  let published = false
  try {
    await Promise.all([
      writeFile(join(staging, 'manifest.json'), serialized.manifest, { flag: 'wx' }),
      writeFile(join(staging, 'history.jsonl'), serialized.history, { flag: 'wx' }),
      writeFile(join(staging, 'schedule.json'), serialized.schedule, { flag: 'wx' }),
      writeFile(join(staging, 'checks.json'), serialized.checks, { flag: 'wx' }),
    ])
    await rename(staging, resultsDir)
    published = true
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    if (code === 'EEXIST' || code === 'ENOTEMPTY') {
      throw new Error(`refusing to overwrite results directory ${resultsDir}`)
    }
    throw error
  } finally {
    if (!published) await rm(staging, { recursive: true, force: true })
  }
}
