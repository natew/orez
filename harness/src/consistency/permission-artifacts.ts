// Strict, collision-safe writer for the permission-transition evidence bundle.
// It persists the typed permission history (schema v1), the checks-v2 verdict
// (pass/fail/inconclusive), the input/profile/topology manifest with build
// provenance and a deterministic replay command, and the (empty) fault schedule.
// Before publication it re-runs the checker over the persisted history and
// refuses to write if the recorded verdict disagrees, so a bundle can never
// claim a result its own history does not support. It never overwrites an
// existing results directory and stages every file under a unique temp dir
// renamed into place atomically, mirroring writeConsistencyArtifacts.
import { mkdir, mkdtemp, rename, rm, stat, writeFile } from 'node:fs/promises'
import { basename, dirname, join } from 'node:path'

import { validateFaultSchedule, type FaultSchedule } from './fault-schedule.js'
import { assertLosslessJsonValue } from './json-value.js'
import {
  checkPermissionTransition,
  classifyPermissionOutcome,
  PERMISSION_CHECKS_SCHEMA_VERSION,
  PERMISSION_HISTORY_SCHEMA_VERSION,
  PERMISSION_TRANSITION_PROFILE,
  PERMISSION_TRANSITION_PROFILE_VERSION,
  type PermissionEvent,
  type PermissionOutcome,
} from './permission-transition.js'

export type PermissionManifest = {
  schemaVersion: typeof PERMISSION_CHECKS_SCHEMA_VERSION
  kind: 'orez-permission-transition'
  runId: string
  seed: { value: string; source: 'fixed' | 'random' | 'replay' }
  profile: {
    name: string
    version: number
    historySchemaVersion: number
    checksSchemaVersion: number
  }
  host: string
  namespaces: { transition: string; stable: string }
  target: { name: string; build: string }
  replay: { command: string }
}

export type PermissionCheck = {
  name: string
  version: string
  valid: boolean
  violations: string[]
}

export type PermissionChecksArtifact = {
  schemaVersion: typeof PERMISSION_CHECKS_SCHEMA_VERSION
  kind: 'orez-permission-transition-checks'
  result: PermissionOutcome
  checks: PermissionCheck[]
}

export type WritePermissionArtifactsOptions = {
  resultsDir: string
  manifest: PermissionManifest
  history: readonly PermissionEvent[]
  schedule: FaultSchedule
  checks: PermissionChecksArtifact
}

function nonempty(value: unknown, label: string): void {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${label} is empty`)
  }
}

// Every persisted object is a closed record: exactly the allowed keys, no more.
// An unknown key means the writer and the schema have drifted, so we refuse
// rather than silently persist a field nothing validated.
function assertExactKeys(
  value: unknown,
  allowed: readonly string[],
  label: string
): void {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${label} is not an object`)
  }
  const keys = Object.keys(value as Record<string, unknown>)
  const permitted = new Set(allowed)
  for (const key of keys) {
    if (!permitted.has(key)) throw new Error(`${label} has unknown key ${key}`)
  }
  for (const key of allowed) {
    if (!(key in (value as Record<string, unknown>))) {
      throw new Error(`${label} is missing key ${key}`)
    }
  }
}

function validateManifest(manifest: PermissionManifest): void {
  assertExactKeys(
    manifest,
    [
      'schemaVersion',
      'kind',
      'runId',
      'seed',
      'profile',
      'host',
      'namespaces',
      'target',
      'replay',
    ],
    'manifest'
  )
  assertExactKeys(manifest.seed, ['value', 'source'], 'manifest seed')
  assertExactKeys(
    manifest.profile,
    ['name', 'version', 'historySchemaVersion', 'checksSchemaVersion'],
    'manifest profile'
  )
  assertExactKeys(manifest.namespaces, ['transition', 'stable'], 'manifest namespaces')
  assertExactKeys(manifest.target, ['name', 'build'], 'manifest target')
  assertExactKeys(manifest.replay, ['command'], 'manifest replay')
  if (manifest.schemaVersion !== PERMISSION_CHECKS_SCHEMA_VERSION) {
    throw new Error(`manifest has schema version ${manifest.schemaVersion}`)
  }
  if (manifest.kind !== 'orez-permission-transition') {
    throw new Error(`manifest has kind ${String(manifest.kind)}`)
  }
  nonempty(manifest.runId, 'manifest runId')
  nonempty(manifest.seed?.value, 'manifest seed value')
  if (!['fixed', 'random', 'replay'].includes(manifest.seed?.source)) {
    throw new Error(`manifest has invalid seed source ${String(manifest.seed?.source)}`)
  }
  if (
    manifest.profile?.name !== PERMISSION_TRANSITION_PROFILE.name ||
    manifest.profile.version !== PERMISSION_TRANSITION_PROFILE_VERSION ||
    manifest.profile.historySchemaVersion !== PERMISSION_HISTORY_SCHEMA_VERSION ||
    manifest.profile.checksSchemaVersion !== PERMISSION_CHECKS_SCHEMA_VERSION
  ) {
    throw new Error('manifest profile does not match the frozen v1 profile')
  }
  nonempty(manifest.host, 'manifest host')
  nonempty(manifest.namespaces?.transition, 'manifest transition namespace')
  nonempty(manifest.namespaces?.stable, 'manifest stable namespace')
  if (manifest.namespaces.transition === manifest.namespaces.stable) {
    throw new Error('manifest namespaces are not distinct')
  }
  nonempty(manifest.target?.name, 'manifest target name')
  nonempty(manifest.target?.build, 'manifest build provenance')
  nonempty(manifest.replay?.command, 'manifest replay command')
}

function validateChecks(checks: PermissionChecksArtifact): void {
  assertExactKeys(
    checks,
    ['schemaVersion', 'kind', 'result', 'checks'],
    'checks envelope'
  )
  if (checks.schemaVersion !== PERMISSION_CHECKS_SCHEMA_VERSION) {
    throw new Error(`checks have schema version ${checks.schemaVersion}`)
  }
  if (checks.kind !== 'orez-permission-transition-checks') {
    throw new Error(`checks have kind ${String(checks.kind)}`)
  }
  if (!['pass', 'fail', 'inconclusive'].includes(checks.result)) {
    throw new Error(`checks have invalid result ${String(checks.result)}`)
  }
  // the frozen v1 profile is a single named check; there is exactly one
  if (!Array.isArray(checks.checks) || checks.checks.length !== 1) {
    throw new Error('checks artifact must carry exactly one frozen profile check')
  }
  const check = checks.checks[0]!
  assertExactKeys(check, ['name', 'version', 'valid', 'violations'], 'check')
  if (check.name !== PERMISSION_TRANSITION_PROFILE.name) {
    throw new Error(`check name ${check.name} is not the frozen profile`)
  }
  if (check.version !== String(PERMISSION_TRANSITION_PROFILE_VERSION)) {
    throw new Error(`check version ${check.version} is not the frozen profile version`)
  }
  if (!Array.isArray(check.violations)) {
    throw new Error(`check ${check.name} has malformed violations`)
  }
  if (check.violations.some((v) => typeof v !== 'string' || v.trim() === '')) {
    throw new Error(`check ${check.name} has an empty violation`)
  }
  if (typeof check.valid !== 'boolean') {
    throw new Error(`check ${check.name} has invalid valid flag`)
  }
  if (check.valid !== (check.violations.length === 0)) {
    throw new Error(`check ${check.name} validity disagrees with its violations`)
  }
  if ((checks.result === 'pass') !== check.valid) {
    throw new Error('checks result disagrees with its check validity')
  }
}

// The integrity gate: recompute the verdict from the persisted history and
// refuse to publish a bundle whose recorded checks disagree with what its own
// history produces.
function validateHistoryAgainstChecks(
  history: readonly PermissionEvent[],
  checks: PermissionChecksArtifact
): void {
  if (history.length === 0) throw new Error('permission history is empty')
  const recomputed = checkPermissionTransition(history)
  const recorded = checks.checks[0]!
  if (
    recorded.valid !== recomputed.valid ||
    JSON.stringify(recorded.violations) !== JSON.stringify(recomputed.violations)
  ) {
    throw new Error('recorded checks do not match the persisted history')
  }
  if (classifyPermissionOutcome(history, recomputed) !== checks.result) {
    throw new Error('recorded result does not match the persisted history')
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

export async function writePermissionArtifacts(
  options: WritePermissionArtifactsOptions
): Promise<void> {
  const { resultsDir, manifest, history, schedule, checks } = options
  await refuseExistingDirectory(resultsDir)
  validateManifest(manifest)
  validateChecks(checks)
  validateHistoryAgainstChecks(history, checks)

  const scheduleResult = validateFaultSchedule(schedule)
  if (!scheduleResult.valid) {
    throw new Error(
      `cannot write invalid schedule:\n${scheduleResult.violations.join('\n')}`
    )
  }
  // this profile declares no faults; the bundle records that explicitly
  if (
    schedule.faultsRequired ||
    schedule.plans.length !== 0 ||
    schedule.receipts.length !== 0
  ) {
    throw new Error('permission transition runs an empty fault schedule only')
  }

  assertLosslessJsonValue(manifest, 'manifest')
  assertLosslessJsonValue(history, 'history')
  assertLosslessJsonValue(schedule, 'schedule')
  assertLosslessJsonValue(checks, 'checks')

  // serialize before reserving any filesystem path so an invalid runtime
  // payload cannot strand an empty final directory
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
