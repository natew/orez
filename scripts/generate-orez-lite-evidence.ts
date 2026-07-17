import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'

export type Status = 'verified' | 'unverified'

type Suite = {
  id: string
  name: string
  host: string
  status: 'pass' | 'awaiting'
  scenarioCount: number | null
  randomizedSeed: number | null
  operationCount: number | null
  operationUnit: string
  restarts: number | null
  durationMs: number | null
  whatItProves: string
  whatItDoesNotProve: string
  commands: string[]
  logsUrl: string | null
  artifactsUrl: string | null
}

export type Evidence = {
  schemaVersion: number
  status: Status
  release: { version: string; tag: string; sha: string | null; url: string }
  build: { sha: string | null; url: string | null }
  versions: Record<string, string | null>
  qualification: {
    qualifiedAt: string | null
    lastGreen: { runId: number; url: string } | null
    scenarioCount: number | null
    randomizedSeed: number | null
    operationCount: number | null
    operationUnit: string | null
    restarts: number | null
    durationMs: number | null
  }
  supportedContracts: string[]
  compatibility: { host: string; status: 'pass' | 'awaiting'; contract: string }[]
  suites: Suite[]
  artifacts: {
    evidenceJsonUrl: string | null
    logsUrl: string | null
    regressionTracesUrl: string | null
    regressionTraceCount: number
    retentionDays: number
  }
  reproduction: { environment: string[]; full: string[] }
  knownLimitations: string[]
  unresolvedLanes: { name: string; status: string; detail: string; url: string }[]
  gate: { workflow: string; branch: string; requiredJobs: string[]; policy: string }
}

type ActionsRun = {
  event: string
  head_branch: string
  head_sha: string
  html_url: string
}

type ActionsJob = {
  name: string
  status: string
  conclusion: string | null
  started_at: string
  completed_at: string | null
  html_url: string
}

const root = resolve(import.meta.dir, '..')
const evidencePath = join(root, 'site/data/orez-lite-evidence.json')
const githubRepository = process.env.GITHUB_REPOSITORY ?? 'natew/orez'
const repositoryUrl = `https://github.com/${githubRepository}`

function readJson<T>(path: string): T {
  return JSON.parse(readFileSync(path, 'utf8')) as T
}

function packageVersion(name: string): string {
  const lock = readFileSync(join(root, 'Cargo.lock'), 'utf8')
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const match = lock.match(
    new RegExp(`\\[\\[package\\]\\]\\nname = "${escaped}"\\nversion = "([^"]+)"`)
  )
  if (!match?.[1]) throw new Error(`Cargo.lock does not contain ${name}`)
  return match[1]
}

function rustVersion(): string {
  const toolchain = readFileSync(join(root, 'rust-toolchain.toml'), 'utf8')
  const match = toolchain.match(/^channel\s*=\s*"([^"]+)"/m)
  if (!match?.[1]) throw new Error('rust-toolchain.toml has no channel')
  return match[1]
}

function walkFiles(directory: string, predicate: (path: string) => boolean): string[] {
  if (!existsSync(directory)) return []
  const files: string[] = []
  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry)
    const stat = statSync(path)
    if (stat.isDirectory()) files.push(...walkFiles(path, predicate))
    else if (predicate(path)) files.push(path)
  }
  return files
}

function rustTestCount(): number {
  return walkFiles(join(root, 'crates'), (path) => path.endsWith('.rs')).reduce(
    (count, path) =>
      count +
      (readFileSync(path, 'utf8').match(/#\[(?:tokio::)?test(?:\([^\]]*\))?\]/g)
        ?.length ?? 0),
    0
  )
}

// distinct harness lanes (bun src/<name>.ts) a ci job runs. the evidence job
// checks out the same workflow after the lanes pass, so this tracks lane
// additions and removals automatically instead of a point-in-time constant.
export function laneCountFromWorkflow(workflow: string, jobName: string): number {
  const lines = workflow.split('\n')
  const start = lines.indexOf(`  ${jobName}:`)
  if (start === -1) throw new Error(`ci.yml has no job ${jobName}`)
  let end = lines.length
  for (let i = start + 1; i < lines.length; i++) {
    if (/^ {2}\S/.test(lines[i])) {
      end = i
      break
    }
  }
  const block = lines.slice(start, end).join('\n')
  const lanes = new Set<string>()
  for (const match of block.matchAll(/\bbun\s+src\/([\w.-]+)\.ts\b/g)) {
    lanes.add(match[1])
  }
  if (lanes.size === 0) throw new Error(`ci.yml job ${jobName} runs no harness lanes`)
  return lanes.size
}

function ciLaneCount(jobName: string): number {
  return laneCountFromWorkflow(
    readFileSync(join(root, '.github/workflows/ci.yml'), 'utf8'),
    jobName
  )
}

type StateMachineRun = { target: string; trace: { kind: string }[] }

const restartKinds = new Set(['serverRestart', 'clientRestart'])

// total generated lifecycle steps across a target's recorded state-machine
// traces. this is the only artifact-backed operation count the fault jobs
// upload; sweep rounds, protocol-fuzz cases, and push-soak operations run but
// emit no per-run artifact, so they stay out of the counted total.
export function traceStepCount(runs: StateMachineRun[]): number {
  return runs.reduce((total, run) => total + run.trace.length, 0)
}

// process restarts recorded in a target's state-machine traces.
export function traceRestartCount(runs: StateMachineRun[]): number {
  return runs.reduce(
    (total, run) => total + run.trace.filter((op) => restartKinds.has(op.kind)).length,
    0
  )
}

// state-machine result artifacts downloaded from the fault jobs. each fault job
// runs the lifecycle state machine and uploads results/, so no run for a target
// means the artifact download broke; fail rather than publish a false 0.
function stateMachineRuns(resultsDir: string, target: string): StateMachineRun[] {
  const runs = walkFiles(resultsDir, (path) => /state-machine-.*\.json$/.test(path))
    .map((path) => readJson<StateMachineRun>(path))
    .filter((run) => run.target === target)
  if (runs.length === 0) {
    throw new Error(`no state-machine results for ${target} under ${resultsDir}`)
  }
  return runs
}

function sqliteVersion(libsqliteVersion: string): string {
  const cargoHome = process.env.CARGO_HOME ?? join(process.env.HOME ?? '', '.cargo')
  const registryRoot = join(cargoHome, 'registry/src')

  const findVersion = () => {
    if (!existsSync(registryRoot)) return null
    for (const registry of readdirSync(registryRoot)) {
      const path = join(
        registryRoot,
        registry,
        `libsqlite3-sys-${libsqliteVersion}`,
        'sqlite3/sqlite3.c'
      )
      if (!existsSync(path)) continue
      const match = readFileSync(path, 'utf8').match(
        /^#define SQLITE_VERSION\s+"([^"]+)"/m
      )
      if (match?.[1]) return match[1]
    }
    return null
  }

  let version = findVersion()
  if (version) return version

  const fetched = Bun.spawnSync(['cargo', 'fetch', '--locked'], { cwd: root })
  if (fetched.exitCode !== 0) {
    throw new Error(`cargo fetch failed: ${fetched.stderr.toString()}`)
  }
  version = findVersion()
  if (!version)
    throw new Error(`could not determine bundled SQLite for ${libsqliteVersion}`)
  return version
}

function durationMs(job: ActionsJob): number {
  if (!job.completed_at) throw new Error(`${job.name} has no completion timestamp`)
  return new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()
}

async function githubJson<T>(path: string): Promise<T> {
  const token = process.env.GITHUB_TOKEN
  if (!token) throw new Error('GITHUB_TOKEN is required to generate verified evidence')
  const response = await fetch(
    `${process.env.GITHUB_API_URL ?? 'https://api.github.com'}${path}`,
    {
      headers: {
        Accept: 'application/vnd.github+json',
        Authorization: `Bearer ${token}`,
        'X-GitHub-Api-Version': '2022-11-28',
      },
    }
  )
  if (!response.ok) throw new Error(`GitHub API ${path} returned ${response.status}`)
  return (await response.json()) as T
}

async function releaseSha(tag: string): Promise<string> {
  const ref = await githubJson<{ object: { sha: string; type: string; url: string } }>(
    `/repos/${githubRepository}/git/ref/tags/${tag}`
  )
  if (ref.object.type === 'commit') return ref.object.sha
  const tagObject = await githubJson<{ object: { sha: string } }>(
    new URL(ref.object.url).pathname.replace(
      /^\/repos\/[^/]+\/[^/]+/,
      `/repos/${githubRepository}`
    )
  )
  return tagObject.object.sha
}

export function statusForQualifiedBuild(buildSha: string | null): Status {
  return buildSha === null ? 'unverified' : 'verified'
}

export function validate(evidence: Evidence, expectedSha?: string): void {
  const errors: string[] = []
  if (evidence.schemaVersion !== 1) errors.push('schemaVersion must be 1')
  if (!/^\d+\.\d+\.\d+$/.test(evidence.release.version)) {
    errors.push('release.version must be exact semver')
  }
  if (!evidence.release.sha || !/^[0-9a-f]{40}$/.test(evidence.release.sha)) {
    errors.push('release.sha must be a full immutable commit SHA')
  }
  for (const key of ['zero', 'rust', 'sqlite', 'rusqlite', 'libsqlite3Sys', 'workerd']) {
    if (!evidence.versions[key]) errors.push(`versions.${key} is required`)
    else if (!/^\d+(?:\.\d+)+(?:[-+][0-9A-Za-z.-]+)?$/.test(evidence.versions[key]!)) {
      errors.push(`versions.${key} must be exact, not a range`)
    }
  }
  if (evidence.suites.length === 0) errors.push('at least one suite is required')
  for (const suite of evidence.suites) {
    if (!suite.whatItProves) errors.push(`${suite.id} needs whatItProves`)
    if (!suite.whatItDoesNotProve) errors.push(`${suite.id} needs whatItDoesNotProve`)
    if (suite.commands.length === 0)
      errors.push(`${suite.id} needs reproduction commands`)
  }

  if (evidence.build.sha !== null && !/^[0-9a-f]{40}$/.test(evidence.build.sha)) {
    errors.push('build.sha must be a full immutable commit SHA when present')
  }
  if (expectedSha && evidence.build.sha !== expectedSha) {
    errors.push(`evidence SHA ${evidence.build.sha} does not match ${expectedSha}`)
  }

  if (evidence.build.sha) {
    if (!evidence.qualification.lastGreen || !evidence.qualification.qualifiedAt) {
      errors.push('build evidence needs a last green run and timestamp')
    }
    for (const field of [
      'scenarioCount',
      'randomizedSeed',
      'operationCount',
      'restarts',
      'durationMs',
    ] as const) {
      if (evidence.qualification[field] === null) {
        errors.push(`build evidence needs qualification.${field}`)
      }
    }
    for (const suite of evidence.suites) {
      if (suite.status !== 'pass') errors.push(`${suite.id} is not passing`)
      if (suite.scenarioCount === null || suite.durationMs === null) {
        errors.push(`${suite.id} is missing count or duration`)
      }
      if (!suite.logsUrl || !suite.artifactsUrl) {
        errors.push(`${suite.id} is missing immutable links`)
      }
    }
  }

  if (evidence.status === 'verified') {
    if (!evidence.build.sha) errors.push('verified evidence needs a full build SHA')
    if (evidence.supportedContracts.length === 0) {
      errors.push('verified build evidence needs supported contracts')
    }
    if (evidence.compatibility.some((row) => row.status !== 'pass')) {
      errors.push('every compatibility row must pass')
    }
  } else if (evidence.supportedContracts.length > 0) {
    errors.push('unverified evidence cannot advertise supported contracts')
  }

  if (errors.length) throw new Error(`invalid evidence ledger:\n- ${errors.join('\n- ')}`)
}

async function generate(): Promise<void> {
  const sha = process.env.GITHUB_SHA
  const runId = Number(process.env.GITHUB_RUN_ID)
  if (!sha || !/^[0-9a-f]{40}$/.test(sha) || !Number.isSafeInteger(runId)) {
    throw new Error('GITHUB_SHA and GITHUB_RUN_ID are required')
  }

  const evidence = readJson<Evidence>(evidencePath)
  const rootPackage = readJson<{
    version: string
    devDependencies: Record<string, string>
  }>(join(root, 'package.json'))
  const packageTag = `v${rootPackage.version}`
  const pushedTag = process.env.GITHUB_REF?.startsWith('refs/tags/')
    ? process.env.GITHUB_REF.slice('refs/tags/'.length)
    : null
  if (pushedTag && pushedTag !== packageTag) {
    throw new Error(
      `release tag ${pushedTag} does not match package version ${rootPackage.version}`
    )
  }
  const releaseTag = pushedTag ?? packageTag
  const releaseVersion = releaseTag.startsWith('v') ? releaseTag.slice(1) : releaseTag
  const [run, jobsResponse] = await Promise.all([
    githubJson<ActionsRun>(`/repos/${githubRepository}/actions/runs/${runId}`),
    githubJson<{ jobs: ActionsJob[] }>(
      `/repos/${githubRepository}/actions/runs/${runId}/jobs?per_page=100`
    ),
  ])

  const isMainBuild = run.head_branch === evidence.gate.branch
  const isReleaseTagBuild = process.env.GITHUB_REF === `refs/tags/${releaseTag}`
  if (run.event !== 'push' || (!isMainBuild && !isReleaseTagBuild)) {
    throw new Error(
      `only a push to ${evidence.gate.branch} or ${releaseTag} can publish build evidence`
    )
  }
  if (run.head_sha !== sha) {
    throw new Error(`workflow SHA ${run.head_sha} does not match checkout SHA ${sha}`)
  }

  const jobs = new Map(jobsResponse.jobs.map((job) => [job.name, job]))
  const required = evidence.gate.requiredJobs.map((name) => {
    const job = jobs.get(name)
    if (!job) throw new Error(`required job ${name} is missing from run ${runId}`)
    if (job.status !== 'completed' || job.conclusion !== 'success') {
      throw new Error(`required job ${name} is ${job.status}/${job.conclusion}`)
    }
    return job
  })

  const cfPackage = readJson<{ devDependencies: Record<string, string> }>(
    join(root, 'packages/sync-cf-host/package.json')
  )
  const libsqlite3Sys = packageVersion('libsqlite3-sys')
  const seed = runId
  const artifactsUrl = `${run.html_url}#artifacts`
  const regressionTracesUrl = process.env.OREZ_REGRESSION_TRACES_URL
  if (
    !regressionTracesUrl?.startsWith(`${repositoryUrl}/actions/runs/${runId}/artifacts/`)
  ) {
    throw new Error(
      "OREZ_REGRESSION_TRACES_URL must identify this run's uploaded artifact"
    )
  }
  const job = (name: string) => jobs.get(name)!
  const suite = (id: string) => {
    const result = evidence.suites.find((item) => item.id === id)
    if (!result) throw new Error(`suite ${id} is missing from the template`)
    return result
  }

  evidence.release.version = releaseVersion
  evidence.release.tag = releaseTag
  evidence.release.sha = await releaseSha(evidence.release.tag)
  evidence.release.url = `${repositoryUrl}/tree/${evidence.release.tag}`
  evidence.build = { sha, url: `${repositoryUrl}/tree/${sha}` }
  evidence.status = statusForQualifiedBuild(sha)
  if (isReleaseTagBuild && evidence.release.sha !== sha) {
    throw new Error(
      `release tag ${releaseTag} resolves to ${evidence.release.sha}, not checkout SHA ${sha}`
    )
  }
  evidence.versions = {
    zero: rootPackage.devDependencies['@rocicorp/zero'],
    rust: rustVersion(),
    sqlite: sqliteVersion(libsqlite3Sys),
    rusqlite: packageVersion('rusqlite'),
    libsqlite3Sys,
    workerd: cfPackage.devDependencies.workerd,
    wrangler: cfPackage.devDependencies.wrangler,
    bun: Bun.version,
  }

  // the fault jobs upload harness/results/ + harness/regressions/; the evidence
  // job downloads both into OREZ_REGRESSION_TRACES_PATH, so results/ holds the
  // recorded state-machine traces this run produced for each host.
  const traceRoot = resolve(
    root,
    process.env.OREZ_REGRESSION_TRACES_PATH ?? 'harness/regressions'
  )
  const stateMachineResultsDir = join(traceRoot, 'results')
  const rustLocalStateMachine = stateMachineRuns(stateMachineResultsDir, 'rust-local')
  const rustCfStateMachine = stateMachineRuns(stateMachineResultsDir, 'rust-cf')

  const rustCore = suite('rust-core')
  Object.assign(rustCore, {
    status: 'pass',
    scenarioCount: rustTestCount(),
    randomizedSeed: null,
    operationCount: null,
    restarts: 0,
    durationMs: durationMs(job('rust')),
    logsUrl: job('rust').html_url,
    artifactsUrl,
  })

  const nativeHost = suite('native-host')
  Object.assign(nativeHost, {
    status: 'pass',
    scenarioCount: ciLaneCount('rust-local'),
    randomizedSeed: seed,
    // heterogeneous per-lane totals (sweep rounds, storm clients, query shapes)
    // are not summable into one honest operation count.
    operationCount: null,
    restarts: 0,
    durationMs: durationMs(job('rust-local')),
    logsUrl: job('rust-local').html_url,
    artifactsUrl,
  })

  const wasmWorkerd = suite('wasm-workerd')
  Object.assign(wasmWorkerd, {
    status: 'pass',
    scenarioCount: ciLaneCount('sync-cf-host'),
    randomizedSeed: seed,
    operationCount: traceStepCount(rustCfStateMachine),
    restarts: traceRestartCount(rustCfStateMachine),
    durationMs: durationMs(job('sync-cf-host')),
    logsUrl: job('sync-cf-host').html_url,
    artifactsUrl,
  })

  const nativeFaultRecovery = suite('native-fault-recovery')
  Object.assign(nativeFaultRecovery, {
    status: 'pass',
    scenarioCount: ciLaneCount('rust-local-faults'),
    randomizedSeed: seed,
    operationCount: traceStepCount(rustLocalStateMachine),
    restarts: traceRestartCount(rustLocalStateMachine),
    durationMs: durationMs(job('rust-local-faults')),
    logsUrl: job('rust-local-faults').html_url,
    artifactsUrl,
  })

  const stockDifferential = suite('stock-zero-differential')
  Object.assign(stockDifferential, {
    status: 'pass',
    scenarioCount: ciLaneCount('harness'),
    randomizedSeed: seed,
    // per-query comparisons are not tallied into a single operation total.
    operationCount: null,
    restarts: 0,
    durationMs: durationMs(job('harness')) + durationMs(job('rust-local')),
    logsUrl: job('rust-local').html_url,
    artifactsUrl,
  })

  for (const row of evidence.compatibility) row.status = 'pass'
  const qualifiedContracts = [
    'snapshot and incremental pull cookies',
    'idempotent custom-mutator push and mutation IDs',
    'query membership and permission grant/revoke',
    'wake hints with pull-based convergence',
    'native SQLite transaction and restart integration',
    'native protocol fuzz, storage-fault recovery, backup/restore, and lifecycle state machine',
    'local workerd WASM value and transaction boundary',
    'local workerd protocol fuzz, recovery faults, backup/restore, and lifecycle state machine',
    'named-query differential against Zero 1.7.0',
  ]
  evidence.supportedContracts = qualifiedContracts

  const started = Math.min(...required.map((item) => new Date(item.started_at).getTime()))
  const completed = Math.max(
    ...required.map((item) => new Date(item.completed_at!).getTime())
  )
  evidence.qualification = {
    qualifiedAt: new Date().toISOString(),
    lastGreen: { runId, url: run.html_url },
    scenarioCount: evidence.suites.reduce(
      (total, item) => total + (item.scenarioCount ?? 0),
      0
    ),
    randomizedSeed: seed,
    operationCount: evidence.suites.reduce(
      (total, item) => total + (item.operationCount ?? 0),
      0
    ),
    operationUnit:
      'recorded generated lifecycle steps from the state-machine trace artifacts; randomized sweep rounds, protocol-fuzz cases, push-soak operations, and SQLite statements are executed but not summed here',
    restarts: evidence.suites.reduce((total, item) => total + (item.restarts ?? 0), 0),
    durationMs: completed - started,
  }

  const regressionRoot = existsSync(join(traceRoot, 'regressions'))
    ? join(traceRoot, 'regressions')
    : traceRoot
  const traces = walkFiles(regressionRoot, (path) => path.endsWith('.json'))
  evidence.artifacts = {
    evidenceJsonUrl: artifactsUrl,
    logsUrl: run.html_url,
    regressionTracesUrl,
    regressionTraceCount: traces.length,
    retentionDays: 90,
  }
  evidence.reproduction.full = evidence.reproduction.full.map((command) =>
    command.replaceAll('<ledger-seed>', String(seed))
  )
  for (const item of evidence.suites) {
    item.commands = item.commands.map((command) =>
      command.replaceAll('<ledger-seed>', String(seed))
    )
  }
  const isVerifiedRelease = evidence.release.sha === sha
  const releaseIdentityLimitation = isVerifiedRelease
    ? []
    : [
        `Build ${sha} passed the required CI jobs and is verified at that exact SHA. It is newer than release ${releaseTag}, which resolves to ${evidence.release.sha}, so it is not labeled as a verified release.`,
      ]
  evidence.knownLimitations = [
    ...releaseIdentityLimitation,
    'The workerd lane is local emulation; deployed Cloudflare isolate memory, quota, eviction, and regional propagation require a separate named deployment qualification.',
    `The compatibility corpus is pinned to Zero ${evidence.versions.zero} and does not imply compatibility with later Zero releases.`,
    'Scenario counts are the distinct harness lanes CI ran per host; the operation total counts only the recorded state-machine lifecycle steps, not every sweep round, protocol-fuzz case, soak push, internal assertion, HTTP request, or SQLite statement.',
    `GitHub Actions logs and artifacts are immutable per run but retained for ${evidence.artifacts.retentionDays} days.`,
  ]
  evidence.unresolvedLanes = [
    ...(isVerifiedRelease
      ? []
      : [
          {
            name: 'Release identity',
            status: 'awaiting',
            detail: `The build is CI-verified, but ${releaseTag} names ${evidence.release.sha} rather than ${sha}.`,
            url: `${repositoryUrl}/compare/${evidence.release.tag}...${sha}`,
          },
        ]),
    {
      name: 'Deployed Cloudflare qualification',
      status: 'fragile',
      detail:
        'The deployed rust-cf query differential and platform-specific soak remain outside the standard CI gate.',
      url: `${repositoryUrl}/blob/${sha}/plans/rust-sync-m6-qualification.md`,
    },
  ]
  evidence.gate.policy =
    'CI publishes verified build evidence only after every required job succeeds at the exact build SHA. It labels that build a verified release only when the immutable release tag resolves to the same SHA.'

  validate(evidence, sha)
  writeFileSync(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`)
  const identity = evidence.release.sha === sha ? 'release' : 'build'
  console.log(
    `generated ${evidence.status} Orez Lite ${identity} evidence for ${sha} from run ${runId}`
  )
}

if (import.meta.main) {
  const mode = process.argv[2] ?? 'check'
  if (mode === 'generate') await generate()
  else if (mode === 'check') {
    const evidence = readJson<Evidence>(evidencePath)
    validate(evidence, process.env.EXPECTED_EVIDENCE_SHA)
    console.log(`valid ${evidence.status} Orez Lite evidence ledger`)
  } else {
    throw new Error('usage: bun scripts/generate-orez-lite-evidence.ts [check|generate]')
  }
}
