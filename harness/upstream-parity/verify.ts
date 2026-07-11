// verify the upstream-parity inventory against the real mono checkout, and
// detect drift from the audited SHA. this is what makes "all existing tests we
// could port" evidence-backed instead of a claim, and what stops parity from
// silently decaying as upstream moves.
//
//   bun harness/upstream-parity/verify.ts                # verify + drift check
//   bun harness/upstream-parity/verify.ts --allow-drift  # non-zero only on a
//                                                          # count/path mismatch
//   MONO=/path/to/mono bun harness/upstream-parity/verify.ts
//
// exits non-zero when the inventory no longer matches upstream (a stale audit)
// or when origin/main has moved past the audited SHA (unless --allow-drift).
// intended to run in a NON-GATING nightly audit, or manually before trusting
// the ledger.
import { execFileSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { homedir } from 'node:os'
import { parseArgs } from 'node:util'

const { values: args } = parseArgs({
  options: {
    'allow-drift': { type: 'boolean', default: false },
    fetch: { type: 'boolean', default: true },
  },
})

const HERE = import.meta.dirname
const inventory = JSON.parse(readFileSync(join(HERE, 'inventory.json'), 'utf-8'))
const ledger = JSON.parse(readFileSync(join(HERE, 'ledger.json'), 'utf-8'))

const MONO = process.env.MONO ?? join(homedir(), 'github', 'mono')
if (!existsSync(join(MONO, '.git'))) {
  console.error(`[verify] mono checkout not found at ${MONO} (set MONO=/path/to/mono)`)
  process.exit(2)
}

function git(...a: string[]): string {
  return execFileSync('git', ['-C', MONO, ...a], { encoding: 'utf-8' }).trim()
}

const failures: string[] = []
const check = (ok: boolean, msg: string) => {
  console.log(`${ok ? '  ok  ' : ' FAIL '} ${msg}`)
  if (!ok) failures.push(msg)
}

const auditedSha: string = inventory.auditedUpstreamSha
console.log(`[verify] mono=${MONO}`)
console.log(`[verify] audited SHA=${auditedSha}`)

if (args.fetch) {
  try {
    git('fetch', 'origin', '--quiet')
  } catch {
    console.log('[verify] (fetch failed — offline? verifying against local refs)')
  }
}

// --- drift: has origin/main moved past the audited SHA? -----------------------
let currentSha = ''
try {
  currentSha = git('rev-parse', 'origin/main')
} catch {
  currentSha = git('rev-parse', 'HEAD')
}
const drifted = currentSha !== auditedSha
console.log(`[verify] current origin/main=${currentSha}${drifted ? '  <-- DRIFTED' : '  (matches audit)'}`)
if (drifted) {
  const commits = git('log', '--oneline', `${auditedSha}..${currentSha}`)
  console.log('[verify] commits since audit:')
  for (const line of commits.split('\n').filter(Boolean)) console.log(`         ${line}`)
  // does the drift touch conformance-relevant test/fuzz paths?
  const touched = git(
    'diff',
    '--name-only',
    `${auditedSha}..${currentSha}`,
    '--',
    'packages/zql-integration-tests',
    'packages/zql',
    'packages/zqlite',
    'packages/zero-protocol',
    'packages/zero-cache'
  )
    .split('\n')
    .filter((f) => /\.(test\.ts)$/.test(f) || f.includes('/fuzz/'))
  if (touched.length) {
    console.log('[verify] drift touches conformance test/fuzz files:')
    for (const f of touched) console.log(`         ~ ${f}`)
  }
}

// --- evidence: the audited SHA must still exist locally to count against -------
let auditRef = auditedSha
try {
  git('cat-file', '-e', `${auditedSha}^{commit}`)
} catch {
  console.log(`[verify] audited SHA not in checkout; counting against origin/main instead`)
  auditRef = currentSha
}

function listTests(dir: string): string[] {
  return git('ls-tree', '-r', '--name-only', auditRef, '--', dir)
    .split('\n')
    .filter((f) => f.endsWith('.test.ts'))
    .sort()
}

// --- 24 zql-integration-tests: exact set equality -----------------------------
const actualZit = listTests('packages/zql-integration-tests/src')
const expectedZit: string[] = inventory.zqlIntegrationTests.files.map((f: { path: string }) => f.path).sort()
const missing = expectedZit.filter((p) => !actualZit.includes(p))
const extra = actualZit.filter((p) => !expectedZit.includes(p))
check(
  actualZit.length === inventory.zqlIntegrationTests.total,
  `zql-integration-tests count: ${actualZit.length} == ${inventory.zqlIntegrationTests.total}`
)
check(missing.length === 0, `no inventoried file missing upstream${missing.length ? ` (missing: ${missing.join(', ')})` : ''}`)
check(extra.length === 0, `no upstream file un-inventoried${extra.length ? ` (extra: ${extra.join(', ')})` : ''}`)

// --- aggregate arithmetic must sum to the total -------------------------------
const byStatus = inventory.zqlIntegrationTests.aggregateByOrezStatus
const statusSum = Object.values(byStatus).reduce((a, b) => a + (b as number), 0)
check(statusSum === inventory.zqlIntegrationTests.total, `orezStatus aggregate sums to total: ${statusSum} == ${inventory.zqlIntegrationTests.total}`)
const byPort = inventory.zqlIntegrationTests.aggregateByPortability
const portSum = Object.values(byPort).reduce((a, b) => a + (b as number), 0)
check(portSum === inventory.zqlIntegrationTests.total, `portability aggregate sums to total: ${portSum} == ${inventory.zqlIntegrationTests.total}`)

// each inventoried orezStatus tally must match the actual per-file classification
const statusCounts: Record<string, number> = {}
for (const f of inventory.zqlIntegrationTests.files as { orezStatus: string }[]) {
  statusCounts[f.orezStatus] = (statusCounts[f.orezStatus] ?? 0) + 1
}
for (const [k, v] of Object.entries(byStatus)) {
  check(statusCounts[k] === v, `orezStatus[${k}] tally matches files: ${statusCounts[k] ?? 0} == ${v}`)
}

// --- zero-cache / zero-protocol / fuzz counts ---------------------------------
const zcCount = listTests('packages/zero-cache/src').length
check(zcCount === inventory.zeroCache.total, `zero-cache test files: ${zcCount} == ${inventory.zeroCache.total}`)
const zcPgCount = git('ls-tree', '-r', '--name-only', auditRef, '--', 'packages/zero-cache/src')
  .split('\n')
  .filter((f) => f.endsWith('.pg.test.ts')).length
check(zcPgCount === inventory.zeroCache.pgTests, `zero-cache pg.test.ts files: ${zcPgCount} == ${inventory.zeroCache.pgTests}`)
check(
  inventory.zeroCache.testPortability['black-box'] + inventory.zeroCache.testPortability['in-process'] === inventory.zeroCache.total,
  `zero-cache testPortability sums to total: ${inventory.zeroCache.testPortability['black-box'] + inventory.zeroCache.testPortability['in-process']} == ${inventory.zeroCache.total}`
)
const zpCount = listTests('packages/zero-protocol/src').length
check(zpCount === inventory.zeroProtocol.total, `zero-protocol test files: ${zpCount} == ${inventory.zeroProtocol.total}`)
const fuzzNonTest = git('ls-tree', '-r', '--name-only', auditRef, '--', 'packages/zql-integration-tests/src/chinook/fuzz')
  .split('\n')
  .filter((f) => f.endsWith('.ts') && !f.endsWith('.test.ts')).length
check(fuzzNonTest === inventory.fuzzModules.nonTestModules, `fuzz non-test modules: ${fuzzNonTest} == ${inventory.fuzzModules.nonTestModules}`)

// --- ledger fuzzModules: 20 entries, one per fuzz file ------------------------
check(ledger.fuzzModules.modules.length === inventory.fuzzModules.totalFiles, `ledger fuzzModules entries: ${ledger.fuzzModules.modules.length} == ${inventory.fuzzModules.totalFiles}`)

// --- every orezArtifact the ledger claims as DONE must exist ------------------
// (a referenced-but-absent file means the status should be in-progress, not
//  ported-black-box / equivalent). check the harness/src/* artifacts the ledger
//  names for ported-black-box entries.
const HARNESS = join(HERE, '..') // harness/
const artifactRefs: string[] = []
for (const m of ledger.fuzzModules.modules as { orezStatus: string; orezArtifact?: string }[]) {
  if (m.orezStatus === 'ported-black-box' && m.orezArtifact) {
    for (const match of m.orezArtifact.matchAll(/harness\/src\/[A-Za-z0-9._-]+\.ts/g)) {
      artifactRefs.push(match[0])
    }
  }
}
for (const ref of [...new Set(artifactRefs)]) {
  const abs = join(HARNESS, ref.replace(/^harness\//, ''))
  check(existsSync(abs), `ported-black-box artifact exists: ${ref}`)
}

// --- report -------------------------------------------------------------------
console.log('')
if (failures.length) {
  console.error(`[verify] STALE AUDIT — ${failures.length} check(s) failed. Re-audit and update the ledger/inventory.`)
  process.exit(1)
}
if (drifted && !args['allow-drift']) {
  console.error('[verify] origin/main has moved past the audited SHA. Re-audit the drift and bump auditedUpstreamSha, or pass --allow-drift to acknowledge.')
  process.exit(1)
}
console.log('[verify] OK — inventory matches upstream at the audited SHA.')
process.exit(0)
