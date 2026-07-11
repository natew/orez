import { mkdir, stat, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'

import { validateFaultSchedule, type FaultSchedule } from './fault-schedule.js'
import { HISTORY_SCHEMA_VERSION, validateHistory, type RunManifest } from './history.js'
import { HistoryRecorder } from './recorder.js'

export type ConsistencyCheck = {
  name: string
  version: string
  input: 'history.jsonl' | 'schedule.json'
  valid: boolean
  violations: string[]
  reports?: string[]
}

export type ConsistencyChecksArtifact = {
  schemaVersion: typeof HISTORY_SCHEMA_VERSION
  kind: 'orez-consistency-checks'
  checks: ConsistencyCheck[]
}

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
  if (manifest.runId.trim() === '') throw new Error('manifest has an empty runId')
  if (manifest.replay.command.trim() === '') {
    throw new Error('manifest has an empty replay command')
  }
}

function validateChecks(checks: ConsistencyChecksArtifact): void {
  if (checks.schemaVersion !== HISTORY_SCHEMA_VERSION) {
    throw new Error(`checks have schema version ${checks.schemaVersion}`)
  }
  if (checks.kind !== 'orez-consistency-checks') {
    throw new Error(`checks have kind ${String(checks.kind)}`)
  }
  for (const [index, check] of checks.checks.entries()) {
    if (check.name.trim() === '') throw new Error(`check ${index} has an empty name`)
    if (check.version.trim() === '')
      throw new Error(`check ${index} has an empty version`)
    if (check.valid !== (check.violations.length === 0)) {
      throw new Error(`check ${check.name} validity disagrees with its violations`)
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

  await mkdir(dirname(resultsDir), { recursive: true })
  try {
    await mkdir(resultsDir)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'EEXIST') {
      throw new Error(`refusing to overwrite results directory ${resultsDir}`)
    }
    throw error
  }

  const json = (value: unknown) => `${JSON.stringify(value, null, 2)}\n`
  const jsonl = history.map((event) => JSON.stringify(event)).join('\n') + '\n'
  await Promise.all([
    writeFile(join(resultsDir, 'manifest.json'), json(manifest), { flag: 'wx' }),
    writeFile(join(resultsDir, 'history.jsonl'), jsonl, { flag: 'wx' }),
    writeFile(join(resultsDir, 'schedule.json'), json(schedule), { flag: 'wx' }),
    writeFile(join(resultsDir, 'checks.json'), json(checks), { flag: 'wx' }),
  ])
}
