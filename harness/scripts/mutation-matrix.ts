// mutation matrix: prove the harness catches real engine bugs.
//
// applies one known-bug patch to the rust engine at a time, runs every
// rust-capable lane against the mutated engine, and records which lanes go
// red. a mutant nothing catches is the point: it names a hole in the net.
// see plans/consistency-hardening-plan.md item 1 and harness/mutants/README.md.
//
//   bun scripts/mutation-matrix.ts                  # baseline + all mutants
//   bun scripts/mutation-matrix.ts --baseline-only
//   bun scripts/mutation-matrix.ts --mutants M1,Q2 --lanes smoke,cargo-sync-core
//
// requires a clean working tree under crates/ (patches are applied and
// reverted with git apply). results land in results/mutation-matrix/<run>/.

import { spawnSync } from 'node:child_process'
import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs'
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
  cmd: string[]
  cwd: string
  timeoutMs: number
}

// every lane here runs against the RUST engine (rust-local target or the
// sync-core cargo suite). lanes that only exercise the typescript core are
// deliberately absent: a rust mutant cannot reach them.
const LANES: Lane[] = [
  {
    name: 'cargo-sync-core',
    cmd: ['cargo', 'test', '-p', 'sync-core'],
    cwd: REPO_ROOT,
    timeoutMs: 25 * 60_000,
  },
  {
    name: 'smoke',
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
    cmd: ['bun', 'src/metamorphic-lane.ts', '--against', 'rust-local'],
    cwd: HARNESS_ROOT,
    timeoutMs: 15 * 60_000,
  },
  {
    name: 'eviction',
    cmd: ['bun', 'src/eviction.ts', '--target', 'rust-local'],
    cwd: HARNESS_ROOT,
    timeoutMs: 15 * 60_000,
  },
  {
    name: 'sweep',
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
    name: 'permissions',
    cmd: ['bun', 'src/permissions.ts', '--target', 'rust-local'],
    cwd: HARNESS_ROOT,
    timeoutMs: 10 * 60_000,
  },
  {
    // one-row diff cap: splits a mutation's row effect from its lmid ack across
    // two pulls, the only system net for the capped-diff cut bugs (M4, O2).
    name: 'capped-diff',
    cmd: ['bun', 'src/capped-diff-lane.ts', '--target', 'rust-local'],
    cwd: HARNESS_ROOT,
    timeoutMs: 5 * 60_000,
  },
]

type Mutant = {
  id: string
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

const laneFilter = args.lanes?.split(',').map((s) => s.trim())
const mutantFilter = args.mutants?.split(',').map((s) => s.trim())
const lanes = LANES.filter((l) => !laneFilter || laneFilter.includes(l.name))
const mutants = manifest.mutants.filter(
  (m) => !mutantFilter || mutantFilter.includes(m.id)
)

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
    env: process.env,
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

function assertCleanEngineTree(context: string) {
  const res = git(['status', '--porcelain', '--', 'crates/'])
  if (res.out.trim() !== '') {
    throw new Error(`crates/ tree is dirty ${context}:\n${res.out}`)
  }
}

// a caught mutant can make proptest persist a regression seed under crates/.
// that seed reproduces the MUTANT, not an engine bug — quarantine it into the
// run's results dir so the tree stays clean and the seed stays inspectable.
function quarantineMutantArtifacts(mutantId: string) {
  const res = git(['status', '--porcelain', '--', 'crates/'])
  for (const line of res.out.split('\n')) {
    if (!line.startsWith('??')) continue
    const rel = line.slice(3).trim()
    const dest = join(resultsDir, `${mutantId}-untracked`, rel.replaceAll('/', '__'))
    mkdirSync(join(resultsDir, `${mutantId}-untracked`), { recursive: true })
    renameSync(join(REPO_ROOT, rel), dest)
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

type MatrixRow = {
  mutant: string
  build: string
  lanes: Record<string, LaneOutcome>
  caughtBy: string[]
}

const matrix: {
  runId: string
  baseline: Record<string, LaneOutcome>
  baselineRedLanes: string[]
  rows: MatrixRow[]
} = { runId, baseline: {}, baselineRedLanes: [], rows: [] }

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

assertCleanEngineTree('before baseline')
console.log(`[matrix] run ${runId} — baseline across ${lanes.length} lanes`)
const baselineBuild = buildEngine('baseline-build.log')
if (baselineBuild.status !== 'pass') {
  throw new Error(`baseline engine build failed, see ${baselineBuild.log}`)
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
  assertCleanEngineTree(`before mutant ${mutant.id}`)
  console.log(`[matrix] mutant ${mutant.id}: ${mutant.description}`)
  applyMutant(mutant)
  const row: MatrixRow = { mutant: mutant.id, build: 'pending', lanes: {}, caughtBy: [] }
  matrix.rows.push(row)
  try {
    const build = buildEngine(`${mutant.id}-build.log`)
    row.build = build.status
    if (build.status !== 'pass') {
      console.log(`[matrix]   build failed — invalid mutant, see ${build.log}`)
      continue
    }
    for (const lane of activeLanes) {
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
  } finally {
    applyMutant(mutant, true)
    quarantineMutantArtifacts(mutant.id)
    assertCleanEngineTree(`after reverting mutant ${mutant.id}`)
  }
  saveMatrix()
}

// a timeout counts as caught above because several mutants (dropped patch
// entries, stalled acks) present as convergence hangs, and every lane passed
// within its budget at baseline. read the lane log before trusting a
// timeout-catch, though: a loaded machine can also blow a budget.

console.log(`[matrix] done -> ${resultsDir}`)
const uncaught = matrix.rows.filter((r) => r.build === 'pass' && r.caughtBy.length === 0)
if (uncaught.length > 0) {
  console.log(`[matrix] UNCAUGHT mutants: ${uncaught.map((r) => r.mutant).join(', ')}`)
}

// --gate: fail when coverage regresses against harness/mutants/expected.json.
// a mutant expected caught that nothing catches is a hole that OPENED; a
// mutant expected uncaught that is now caught means expected.json (and the
// matrix doc) should be updated to ratchet the new coverage in.
if (args.gate) {
  const expected: { caught: Record<string, boolean> } = JSON.parse(
    readFileSync(join(MUTANTS_DIR, 'expected.json'), 'utf8')
  )
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
