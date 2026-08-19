// mutation matrix: prove the harness catches real engine and host bugs.
//
// applies one known-bug patch at a time, runs every compatible lane against
// the mutated target, and records which lanes go red. a mutant nothing catches
// is the point: it names a hole in the net.
// see plans/consistency-hardening-plan.md item 1 and harness/mutants/README.md.
//
//   bun scripts/mutation-matrix.ts                  # baseline + all mutants
//   bun scripts/mutation-matrix.ts --baseline-only
//   bun scripts/mutation-matrix.ts --mutants M1,Q2 --lanes smoke,cargo-sync-core
//
// requires every mutated source file to be clean (patches are applied and
// reverted with git apply). results land in results/mutation-matrix/<run>/.

import { spawnSync } from 'node:child_process'
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'

const HARNESS_ROOT = fileURLToPath(new URL('..', import.meta.url))
const REPO_ROOT = fileURLToPath(new URL('../..', import.meta.url))
const MUTANTS_DIR = join(HARNESS_ROOT, 'mutants')

const { values: args } = parseArgs({
  options: {
    mutants: { type: 'string' },
    lanes: { type: 'string' },
    'baseline-only': { type: 'boolean', default: false },
    'run-id': { type: 'string' },
    gate: { type: 'boolean', default: false },
  },
})

type Lane = {
  name: string
  target: MutantTarget
  cmd: string[]
  cwd: string
  timeoutMs: number
}

const LANES: Lane[] = [
  {
    name: 'cargo-sync-core',
    target: 'rust-engine',
    cmd: ['cargo', 'test', '-p', 'sync-core'],
    cwd: REPO_ROOT,
    timeoutMs: 25 * 60_000,
  },
  {
    name: 'smoke',
    target: 'rust-engine',
    cmd: [
      'bun',
      'src/smoke.ts',
      '--target',
      'rust-local',
      '--clients',
      '10',
      '--projects',
      '2',
    ],
    cwd: HARNESS_ROOT,
    timeoutMs: 10 * 60_000,
  },
  {
    name: 'state-machine',
    target: 'rust-engine',
    cmd: [
      'bun',
      'src/state-machine.ts',
      '--against',
      'rust-local',
      '--seed',
      '7',
      '--steps',
      '24',
    ],
    cwd: HARNESS_ROOT,
    timeoutMs: 15 * 60_000,
  },
  {
    name: 'metamorphic',
    target: 'rust-engine',
    cmd: ['bun', 'src/metamorphic-lane.ts', '--against', 'rust-local'],
    cwd: HARNESS_ROOT,
    timeoutMs: 15 * 60_000,
  },
  {
    name: 'eviction',
    target: 'rust-engine',
    cmd: ['bun', 'src/eviction.ts', '--target', 'rust-local'],
    cwd: HARNESS_ROOT,
    timeoutMs: 15 * 60_000,
  },
  {
    name: 'sweep',
    target: 'rust-engine',
    cmd: [
      'bun',
      'src/sweep.ts',
      '--against',
      'rust-local',
      '--rounds',
      '5',
      '--seed',
      '42',
    ],
    cwd: HARNESS_ROOT,
    timeoutMs: 25 * 60_000,
  },
  {
    // {SEED} is substituted per invocation: the lanes derive their results
    // directory from the seed and refuse to overwrite an existing one, so a
    // reused seed makes every later run fail vacuously at startup.
    name: 'atomic-visibility',
    target: 'rust-engine',
    cmd: [
      'bun',
      'src/atomic-visibility-lane.ts',
      '--target',
      'rust-local',
      '--seed',
      '{SEED}',
    ],
    cwd: HARNESS_ROOT,
    timeoutMs: 10 * 60_000,
  },
  {
    name: 'exactly-once',
    target: 'rust-engine',
    cmd: [
      'bun',
      'src/exactly-once-lmid-lane.ts',
      '--target',
      'rust-local',
      '--seed',
      '{SEED}',
    ],
    cwd: HARNESS_ROOT,
    timeoutMs: 10 * 60_000,
  },
  {
    name: 'orez-lite-host',
    target: 'typescript-host',
    cmd: [
      'bunx',
      'vitest',
      'run',
      '--config',
      'vitest.config.ts',
      'packages/orez-lite/src/cf-do',
    ],
    cwd: REPO_ROOT,
    timeoutMs: 10 * 60_000,
  },
]

type MutantTarget = 'rust-engine' | 'typescript-host'

type Mutant = {
  id: string
  target: MutantTarget
  property: string
  file: string
  description: string
  expectedLanes: string[]
}

type LaneOutcome = {
  status: 'pass' | 'red' | 'timeout' | 'skipped'
  ms: number
  log: string
}

const manifest: { mutants: Mutant[] } = JSON.parse(
  readFileSync(join(MUTANTS_DIR, 'manifest.json'), 'utf8')
)
const expected: { caught: Record<string, boolean> } = JSON.parse(
  readFileSync(join(MUTANTS_DIR, 'expected.json'), 'utf8')
)
const manifestIds = manifest.mutants.map((mutant) => mutant.id)
const manifestIdSet = new Set(manifestIds)
const patchIds = readdirSync(join(MUTANTS_DIR, 'patches'))
  .filter((file) => file.endsWith('.patch'))
  .map((file) => file.slice(0, -'.patch'.length))
const expectedIds = Object.keys(expected.caught)
const inventoryErrors: string[] = []
if (manifestIdSet.size !== manifestIds.length)
  inventoryErrors.push('duplicate manifest id')
for (const id of manifestIds) {
  if (!patchIds.includes(id)) inventoryErrors.push(`missing patch for ${id}`)
  if (expected.caught[id] === undefined)
    inventoryErrors.push(`missing expectation for ${id}`)
}
for (const id of patchIds) {
  if (!manifestIdSet.has(id)) inventoryErrors.push(`orphan patch ${id}`)
}
for (const id of expectedIds) {
  if (!manifestIdSet.has(id)) inventoryErrors.push(`orphan expectation ${id}`)
}
for (const mutant of manifest.mutants) {
  for (const laneName of mutant.expectedLanes) {
    const lane = LANES.find((candidate) => candidate.name === laneName)
    if (!lane) inventoryErrors.push(`${mutant.id} names unknown lane ${laneName}`)
    else if (lane.target !== mutant.target) {
      inventoryErrors.push(`${mutant.id} names incompatible lane ${laneName}`)
    }
  }
}
if (inventoryErrors.length > 0) {
  throw new Error(`invalid mutation inventory:\n${inventoryErrors.join('\n')}`)
}

const mutantFilter = args.mutants?.split(',').map((s) => s.trim())
const unknownMutants = mutantFilter?.filter((id) => !manifestIdSet.has(id)) ?? []
if (unknownMutants.length > 0) {
  throw new Error(`unknown mutant filter: ${unknownMutants.join(', ')}`)
}
const mutants = manifest.mutants.filter(
  (m) => !mutantFilter || mutantFilter.includes(m.id)
)
const targets = new Set(mutants.map((mutant) => mutant.target))
const laneFilter = args.lanes?.split(',').map((s) => s.trim())
const unknownLanes =
  laneFilter?.filter((name) => !LANES.some((candidate) => candidate.name === name)) ?? []
if (unknownLanes.length > 0) {
  throw new Error(`unknown lane filter: ${unknownLanes.join(', ')}`)
}
const lanes = LANES.filter(
  (lane) => targets.has(lane.target) && (!laneFilter || laneFilter.includes(lane.name))
)
if (lanes.length === 0) {
  throw new Error('no compatible mutation lanes selected')
}
const targetsWithoutLanes = [...targets].filter(
  (target) => !lanes.some((lane) => lane.target === target)
)
if (targetsWithoutLanes.length > 0) {
  throw new Error(
    `no compatible mutation lanes selected for: ${targetsWithoutLanes.join(', ')}`
  )
}

const runId = args['run-id'] ?? new Date().toISOString().replace(/[:.]/g, '-')
const resultsDir = join(HARNESS_ROOT, 'results', 'mutation-matrix', runId)
mkdirSync(resultsDir, { recursive: true })

function sh(cmd: string[], cwd: string, timeoutMs: number, logFile: string): LaneOutcome {
  const seed = `${runId}-${logFile.replace(/\.log$/, '')}`
  cmd = cmd.map((part) => part.replaceAll('{SEED}', seed))
  const started = Date.now()
  const res = spawnSync(cmd[0], cmd.slice(1), {
    cwd,
    timeout: timeoutMs,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    // mutant failures are expected evidence, so they must never persist a
    // proptest seed that reproduces patched code instead of the real target.
    env: { ...process.env, PROPTEST_DISABLE_FAILURE_PERSISTENCE: '1' },
  })
  const ms = Date.now() - started
  const log = join(resultsDir, logFile)
  writeFileSync(log, `$ ${cmd.join(' ')}\n\n${res.stdout ?? ''}\n${res.stderr ?? ''}`)
  const timedOut =
    res.error != null && (res.error as NodeJS.ErrnoException).code === 'ETIMEDOUT'
  if (timedOut) return { status: 'timeout', ms, log }
  return { status: res.status === 0 ? 'pass' : 'red', ms, log }
}

function git(argv: string[]): { ok: boolean; out: string } {
  const res = spawnSync('git', argv, { cwd: REPO_ROOT, encoding: 'utf8' })
  return { ok: res.status === 0, out: `${res.stdout}${res.stderr}` }
}

function assertCleanMutationTargets(context: string) {
  const files = [...new Set(mutants.map((mutant) => mutant.file))]
  const res = git(['status', '--porcelain', '--', ...files])
  if (res.out.trim() !== '') {
    throw new Error(`mutation target is dirty ${context}:\n${res.out}`)
  }
}

function applyMutant(m: Mutant, reverse = false) {
  const patch = join(MUTANTS_DIR, 'patches', `${m.id}.patch`)
  const argv = ['apply', ...(reverse ? ['-R'] : []), patch]
  const res = git(argv)
  if (!res.ok) throw new Error(`git ${argv.join(' ')} failed:\n${res.out}`)
}

function buildEngine(logFile: string): LaneOutcome {
  return sh(
    ['cargo', 'build', '--release', '-p', 'sync-native', '--bin', 'sync-native-fixture'],
    REPO_ROOT,
    20 * 60_000,
    logFile
  )
}

function buildHost(logFile: string): LaneOutcome {
  return sh(
    ['bun', 'run', '--cwd', 'packages/orez-lite', 'typecheck'],
    REPO_ROOT,
    10 * 60_000,
    logFile
  )
}

function buildTarget(target: MutantTarget, logFile: string): LaneOutcome {
  return target === 'rust-engine' ? buildEngine(logFile) : buildHost(logFile)
}

type MatrixRow = {
  mutant: string
  build: string
  lanes: Record<string, LaneOutcome>
  caughtBy: string[]
}

const matrix: {
  runId: string
  baselineBuilds: Partial<Record<MutantTarget, LaneOutcome>>
  baseline: Record<string, LaneOutcome>
  baselineRedLanes: string[]
  rows: MatrixRow[]
} = { runId, baselineBuilds: {}, baseline: {}, baselineRedLanes: [], rows: [] }

function saveMatrix() {
  writeFileSync(join(resultsDir, 'matrix.json'), JSON.stringify(matrix, null, 2))
  writeFileSync(join(resultsDir, 'matrix.md'), renderMarkdown())
}

function renderMarkdown(): string {
  const laneNames = lanes
    .map((l) => l.name)
    .filter((l) => !matrix.baselineRedLanes.includes(l))
  const lines: string[] = []
  lines.push(`# Mutation matrix — run ${matrix.runId}`)
  lines.push('')
  if (matrix.baselineRedLanes.length > 0) {
    lines.push(
      `Excluded (red at baseline, cannot attribute a catch): ${matrix.baselineRedLanes.join(', ')}`
    )
    lines.push('')
  }
  lines.push(`| mutant | ${laneNames.join(' | ')} | caught by |`)
  lines.push(`|---|${laneNames.map(() => '---').join('|')}|---|`)
  for (const row of matrix.rows) {
    const cells = laneNames.map((l) => {
      const o = row.lanes[l]
      if (!o) return '·'
      return o.status === 'red' ? 'CAUGHT' : o.status === 'pass' ? 'missed' : o.status
    })
    const caught =
      row.build !== 'pass'
        ? 'build failed (invalid mutant)'
        : row.caughtBy.length > 0
          ? row.caughtBy.join(', ')
          : '**NOTHING**'
    lines.push(`| ${row.mutant} | ${cells.join(' | ')} | ${caught} |`)
  }
  lines.push('')
  return lines.join('\n')
}

// ---- baseline -------------------------------------------------------------

assertCleanMutationTargets('before baseline')
console.log(`[matrix] run ${runId} — baseline across ${lanes.length} lanes`)
for (const target of targets) {
  const build = buildTarget(target, `baseline-${target}-build.log`)
  matrix.baselineBuilds[target] = build
  if (build.status !== 'pass') {
    throw new Error(`baseline ${target} build failed, see ${build.log}`)
  }
}
for (const lane of lanes) {
  const outcome = sh(lane.cmd, lane.cwd, lane.timeoutMs, `baseline-${lane.name}.log`)
  matrix.baseline[lane.name] = outcome
  if (outcome.status !== 'pass') matrix.baselineRedLanes.push(lane.name)
  console.log(
    `[matrix] baseline ${lane.name}: ${outcome.status} (${Math.round(outcome.ms / 1000)}s)`
  )
  saveMatrix()
}

if (args['baseline-only']) {
  console.log(`[matrix] baseline done -> ${resultsDir}`)
  process.exit(matrix.baselineRedLanes.length === 0 ? 0 : 1)
}

// ---- mutants ----------------------------------------------------------------

const activeLanes = lanes.filter((l) => !matrix.baselineRedLanes.includes(l.name))
for (const mutant of mutants) {
  assertCleanMutationTargets(`before mutant ${mutant.id}`)
  console.log(`[matrix] mutant ${mutant.id}: ${mutant.description}`)
  applyMutant(mutant)
  const row: MatrixRow = { mutant: mutant.id, build: 'pending', lanes: {}, caughtBy: [] }
  matrix.rows.push(row)
  try {
    const build = buildTarget(mutant.target, `${mutant.id}-build.log`)
    row.build = build.status
    if (build.status !== 'pass') {
      console.log(`[matrix]   build failed — invalid mutant, see ${build.log}`)
    } else {
      for (const lane of activeLanes.filter((lane) => lane.target === mutant.target)) {
        const outcome = sh(
          lane.cmd,
          lane.cwd,
          lane.timeoutMs,
          `${mutant.id}-${lane.name}.log`
        )
        row.lanes[lane.name] = outcome
        if (outcome.status === 'red' || outcome.status === 'timeout')
          row.caughtBy.push(lane.name)
        console.log(
          `[matrix]   ${lane.name}: ${outcome.status === 'red' ? 'CAUGHT' : outcome.status} (${Math.round(outcome.ms / 1000)}s)`
        )
        saveMatrix()
      }
    }
  } finally {
    applyMutant(mutant, true)
    assertCleanMutationTargets(`after reverting mutant ${mutant.id}`)
  }
  saveMatrix()
}

// a timeout counts as caught above because several mutants (dropped patch
// entries, stalled acks) present as convergence hangs, and every lane passed
// within its budget at baseline. read the lane log before trusting a
// timeout-catch, though: a loaded machine can also blow a budget.

console.log(`[matrix] done -> ${resultsDir}`)
const invalid = matrix.rows.filter((row) => row.build !== 'pass')
if (invalid.length > 0) {
  console.error(
    `[matrix] INVALID mutants: ${invalid.map((row) => row.mutant).join(', ')}`
  )
  process.exit(1)
}
const uncaught = matrix.rows.filter((r) => r.build === 'pass' && r.caughtBy.length === 0)
if (uncaught.length > 0) {
  console.log(`[matrix] UNCAUGHT mutants: ${uncaught.map((r) => r.mutant).join(', ')}`)
}

// --gate: fail when coverage regresses against harness/mutants/expected.json.
// a mutant expected caught that nothing catches is a hole that OPENED; a
// mutant expected uncaught that is now caught means expected.json (and the
// matrix doc) should be updated to ratchet the new coverage in.
if (args.gate) {
  const regressions: string[] = []
  const improvements: string[] = []
  for (const row of matrix.rows) {
    if (row.build !== 'pass') continue
    const want = expected.caught[row.mutant]
    if (want === undefined) continue
    const got = row.caughtBy.length > 0
    if (want && !got) regressions.push(row.mutant)
    if (!want && got) improvements.push(row.mutant)
  }
  if (improvements.length > 0) {
    console.log(
      `[matrix] coverage IMPROVED (update expected.json): ${improvements.join(', ')}`
    )
  }
  if (regressions.length > 0) {
    console.error(`[matrix] GATE FAILED — coverage regressed: ${regressions.join(', ')}`)
    process.exit(1)
  }
}
